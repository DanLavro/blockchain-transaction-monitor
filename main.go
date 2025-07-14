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
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
	})

	logger.Info("Starting Blockchain Transaction Monitor")

	cfg, err := config.LoadConfig()
	if err != nil {
		logger.WithError(err).Fatal("Failed to load configuration")
	}

	logger.Info("Configuration loaded successfully")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	clientManager := blockchain.NewClientManager()

	bitcoinClient := blockchain.NewBitcoinClient(cfg.Bitcoin, logger)
	clientManager.AddClient("bitcoin", bitcoinClient)

	ethereumClient := blockchain.NewEthereumClient(cfg.Ethereum, logger)
	clientManager.AddClient("ethereum", ethereumClient)

	solanaClient := blockchain.NewSolanaClient(cfg.Solana, logger)
	clientManager.AddClient("solana", solanaClient)

	if err := clientManager.ConnectAll(ctx); err != nil {
		logger.WithError(err).Fatal("Failed to connect to blockchain clients")
	}
	defer func() {
		if err := clientManager.DisconnectAll(); err != nil {
			logger.WithError(err).Error("Failed to disconnect from blockchain clients")
		}
	}()

	logger.Info("Connected to all blockchain clients successfully")

	monitoringService := monitor.NewService(cfg, clientManager, kafkaProducer, logger)

	if err := monitoringService.Start(ctx); err != nil {
		logger.WithError(err).Fatal("Failed to start monitoring service")
	}

	logger.Info("Blockchain monitoring service started successfully")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	logger.WithField("signal", sig.String()).Info("Received shutdown signal")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Service.ShutdownTimeout)
	defer shutdownCancel()

	logger.Info("Stopping monitoring service...")
	if err := monitoringService.Stop(shutdownCtx); err != nil {
		logger.WithError(err).Error("Error stopping monitoring service")
	}

	cancel()

	logger.Info("Blockchain Transaction Monitor shutdown complete")
}
