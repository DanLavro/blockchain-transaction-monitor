package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

type Config struct {
	Bitcoin  BitcoinConfig  `json:"bitcoin"`
	Ethereum EthereumConfig `json:"ethereum"`
	Solana   SolanaConfig   `json:"solana"`

	Kafka KafkaConfig `json:"kafka"`

	MonitoredAddresses map[string]UserAddresses `json:"monitored_addresses"`

	Service ServiceConfig `json:"service"`
}

type BitcoinConfig struct {
	RPCURL   string `json:"rpc_url"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	Network  string `json:"network"`
}

type EthereumConfig struct {
	RPCURL  string `json:"rpc_url"`
	WSUrl   string `json:"ws_url,omitempty"`
	APIKey  string `json:"api_key,omitempty"`
	Network string `json:"network"`
	ChainID int64  `json:"chain_id"`
}

type SolanaConfig struct {
	RPCURL  string `json:"rpc_url"`
	WSUrl   string `json:"ws_url,omitempty"`
	Network string `json:"network"`
}

type KafkaConfig struct {
	Brokers []string `json:"brokers"`
	Topic   string   `json:"topic"`
	GroupID string   `json:"group_id"`
}

type UserAddresses struct {
	Bitcoin  []string `json:"bitcoin"`
	Ethereum []string `json:"ethereum"`
	Solana   []string `json:"solana"`
}

type ServiceConfig struct {
	LogLevel        string        `json:"log_level"`
	ShutdownTimeout time.Duration `json:"shutdown_timeout"`
	WorkerPoolSize  int           `json:"worker_pool_size"`
	BatchSize       int           `json:"batch_size"`
}

func LoadConfig() (*Config, error) {
	_ = godotenv.Load()

	config := &Config{
		Bitcoin: BitcoinConfig{
			RPCURL:   getEnv("BITCOIN_RPC_URL", "https://bitcoin-mainnet.blockdaemon.com/rpc"),
			Username: getEnv("BITCOIN_RPC_USERNAME", ""),
			Password: getEnv("BITCOIN_RPC_PASSWORD", ""),
			Network:  getEnv("BITCOIN_NETWORK", "mainnet"),
		},
		Ethereum: EthereumConfig{
			RPCURL:  getEnv("ETHEREUM_RPC_URL", "https://ethereum-mainnet.blockdaemon.com/rpc"),
			WSUrl:   getEnv("ETHEREUM_WS_URL", "wss://ethereum-mainnet.blockdaemon.com/ws"),
			APIKey:  getEnv("ETHEREUM_API_KEY", ""),
			Network: getEnv("ETHEREUM_NETWORK", "mainnet"),
			ChainID: getEnvInt64("ETHEREUM_CHAIN_ID", 1),
		},
		Solana: SolanaConfig{
			RPCURL:  getEnv("SOLANA_RPC_URL", "https://solana-mainnet.blockdaemon.com/rpc"),
			WSUrl:   getEnv("SOLANA_WS_URL", "wss://solana-mainnet.blockdaemon.com/ws"),
			Network: getEnv("SOLANA_NETWORK", "mainnet-beta"),
		},
		Kafka: KafkaConfig{
			Brokers: strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
			Topic:   getEnv("KAFKA_TOPIC", "blockchain-transactions"),
			GroupID: getEnv("KAFKA_GROUP_ID", "blockchain-monitor"),
		},
		Service: ServiceConfig{
			LogLevel:        getEnv("LOG_LEVEL", "info"),
			ShutdownTimeout: getEnvDuration("SHUTDOWN_TIMEOUT", 30*time.Second),
			WorkerPoolSize:  getEnvInt("WORKER_POOL_SIZE", 10),
			BatchSize:       getEnvInt("BATCH_SIZE", 100),
		},
	}

	if addressesJSON := getEnv("MONITORED_ADDRESSES", ""); addressesJSON != "" {
		if err := json.Unmarshal([]byte(addressesJSON), &config.MonitoredAddresses); err != nil {
			return nil, fmt.Errorf("failed to parse MONITORED_ADDRESSES: %w", err)
		}
	} else {
		config.MonitoredAddresses = map[string]UserAddresses{
			"user1": {
				Bitcoin:  []string{"1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"},
				Ethereum: []string{"0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"},
				Solana:   []string{"11111111111111111111111111111112"},
			},
		}
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return config, nil
}

func (c *Config) Validate() error {
	if c.Bitcoin.RPCURL == "" {
		return fmt.Errorf("Bitcoin RPC URL is required")
	}
	if c.Ethereum.RPCURL == "" {
		return fmt.Errorf("Ethereum RPC URL is required")
	}
	if c.Solana.RPCURL == "" {
		return fmt.Errorf("Solana RPC URL is required")
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("Kafka brokers are required")
	}
	if c.Kafka.Topic == "" {
		return fmt.Errorf("Kafka topic is required")
	}
	if len(c.MonitoredAddresses) == 0 {
		return fmt.Errorf("no monitored addresses configured")
	}

	level, err := logrus.ParseLevel(c.Service.LogLevel)
	if err != nil {
		return fmt.Errorf("invalid log level: %w", err)
	}
	logrus.SetLevel(level)

	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

func getEnvInt64(key string, defaultValue int64) int64 {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			return i
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return defaultValue
}
