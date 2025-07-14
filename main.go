package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"blockchain-monitor/blockchain"
	"blockchain-monitor/config"
	"blockchain-monitor/kafka"
	"blockchain-monitor/monitor"

	"github.com/sirupsen/logrus"
)

func main() {
	// Set up logger
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
	})

	logger.Info("Starting Blockchain Transaction Monitor")

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		logger.WithError(err).Fatal("Failed to load configuration")
	}

	logger.Info("Configuration loaded successfully")

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize Kafka producer
	kafkaProducer := kafka.NewProducer(cfg.Kafka, logger)
	if err := kafkaProducer.Connect(ctx); err != nil {
		logger.WithError(err).Fatal("Failed to connect to Kafka")
	}
	defer func() {
		if err := kafkaProducer.Disconnect(); err != nil {
			logger.WithError(err).Error("Failed to disconnect from Kafka")
		}
	}()

	logger.Info("Connected to Kafka successfully")

	// Initialize blockchain clients
	clientManager := blockchain.NewClientManager()

	// Add Bitcoin client
	bitcoinClient := blockchain.NewBitcoinClient(cfg.Bitcoin, logger)
	clientManager.AddClient("bitcoin", bitcoinClient)

	// Add Ethereum client
	ethereumClient := blockchain.NewEthereumClient(cfg.Ethereum, logger)
	clientManager.AddClient("ethereum", ethereumClient)

	// Add Solana client
	solanaClient := blockchain.NewSolanaClient(cfg.Solana, logger)
	clientManager.AddClient("solana", solanaClient)

	// Connect to all blockchain clients
	if err := clientManager.ConnectAll(ctx); err != nil {
		logger.WithError(err).Fatal("Failed to connect to blockchain clients")
	}
	defer func() {
		if err := clientManager.DisconnectAll(); err != nil {
			logger.WithError(err).Error("Failed to disconnect from blockchain clients")
		}
	}()

	logger.Info("Connected to all blockchain clients successfully")

	// Initialize monitoring service
	monitoringService := monitor.NewService(cfg, clientManager, kafkaProducer, logger)

	// Start monitoring service
	if err := monitoringService.Start(ctx); err != nil {
		logger.WithError(err).Fatal("Failed to start monitoring service")
	}

	logger.Info("Blockchain monitoring service started successfully")

	// Set up signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	sig := <-sigCh
	logger.WithField("signal", sig.String()).Info("Received shutdown signal")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Service.ShutdownTimeout)
	defer shutdownCancel()

	// Stop monitoring service
	logger.Info("Stopping monitoring service...")
	if err := monitoringService.Stop(shutdownCtx); err != nil {
		logger.WithError(err).Error("Error stopping monitoring service")
	}

	// Cancel main context to stop all operations
	cancel()

	logger.Info("Blockchain Transaction Monitor shutdown complete")
}
