package monitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"blockchain-monitor/blockchain"
	"blockchain-monitor/config"
	"blockchain-monitor/kafka"
	"blockchain-monitor/models"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type Service struct {
	config         *config.Config
	clientManager  *blockchain.ClientManager
	eventPublisher kafka.EventPublisher
	logger         *logrus.Logger

	mu            sync.RWMutex
	running       bool
	lastProcessed map[string]uint64

	stats ServiceStats

	shutdownCh chan struct{}
	doneCh     chan struct{}
}

type ServiceStats struct {
	StartTime              time.Time                  `json:"start_time"`
	TotalTransactionsFound int64                      `json:"total_transactions_found"`
	TotalEventsPublished   int64                      `json:"total_events_published"`
	TotalErrors            int64                      `json:"total_errors"`
	BlockchainStats        map[string]BlockchainStats `json:"blockchain_stats"`
	LastUpdateTime         time.Time                  `json:"last_update_time"`
}

type BlockchainStats struct {
	LastProcessedBlock uint64    `json:"last_processed_block"`
	CurrentBlock       uint64    `json:"current_block"`
	TransactionsFound  int64     `json:"transactions_found"`
	EventsPublished    int64     `json:"events_published"`
	Errors             int64     `json:"errors"`
	LastUpdateTime     time.Time `json:"last_update_time"`
	IsHealthy          bool      `json:"is_healthy"`
}

func NewService(
	config *config.Config,
	clientManager *blockchain.ClientManager,
	eventPublisher kafka.EventPublisher,
	logger *logrus.Logger,
) *Service {
	return &Service{
		config:         config,
		clientManager:  clientManager,
		eventPublisher: eventPublisher,
		logger:         logger,
		lastProcessed:  make(map[string]uint64),
		stats: ServiceStats{
			StartTime:       time.Now(),
			BlockchainStats: make(map[string]BlockchainStats),
		},
		shutdownCh: make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
}

func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("service is already running")
	}
	s.running = true
	s.mu.Unlock()

	s.logger.Info("Starting blockchain monitoring service")

	group, groupCtx := errgroup.WithContext(ctx)

	for name, client := range s.clientManager.GetAllClients() {
		blockchainName := name
		blockchainClient := client

		group.Go(func() error {
			return s.monitorBlockchain(groupCtx, blockchainName, blockchainClient)
		})
	}

	group.Go(func() error {
		return s.reportStats(groupCtx)
	})

	go func() {
		defer close(s.doneCh)
		if err := group.Wait(); err != nil {
			s.logger.WithError(err).Error("Monitoring service encountered an error")
		}
		s.logger.Info("Blockchain monitoring service stopped")
	}()

	return nil
}

func (s *Service) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.mu.Unlock()

	s.logger.Info("Stopping blockchain monitoring service")

	close(s.shutdownCh)

	select {
	case <-s.doneCh:
		s.logger.Info("Blockchain monitoring service stopped gracefully")
		return nil
	case <-ctx.Done():
		s.logger.Warn("Blockchain monitoring service shutdown timed out")
		return ctx.Err()
	}
}

func (s *Service) monitorBlockchain(ctx context.Context, name string, client blockchain.BlockchainClient) error {
	s.logger.WithField("blockchain", name).Info("Starting blockchain monitoring")

	s.updateBlockchainStats(name, func(stats *BlockchainStats) {
		stats.LastUpdateTime = time.Now()
	})

	addresses := s.getMonitoredAddressesForBlockchain(name)
	if len(addresses) == 0 {
		s.logger.WithField("blockchain", name).Warn("No addresses to monitor for blockchain")
		return nil
	}

	s.logger.WithField("blockchain", name).WithField("addresses", len(addresses)).Info("Monitoring addresses")

	return client.SubscribeToBlocks(ctx, func(block interface{}) {
		if err := s.processBlock(ctx, name, client, block, addresses); err != nil {
			s.logger.WithError(err).WithField("blockchain", name).Error("Failed to process block")
			s.incrementErrors(name)
		}
	})
}

func (s *Service) processBlock(ctx context.Context, blockchainName string, client blockchain.BlockchainClient, block interface{}, addresses []string) error {
	startTime := time.Now()

	blockHeight := s.extractBlockHeight(block, blockchainName)

	s.logger.WithField("blockchain", blockchainName).
		WithField("block_height", blockHeight).
		Debug("Processing block")

	s.setLastProcessedBlock(blockchainName, blockHeight)

	events, err := client.ProcessTransactionsInBlock(ctx, block, addresses)
	if err != nil {
		return fmt.Errorf("failed to process transactions in block %d: %w", blockHeight, err)
	}

	if len(events) == 0 {
		s.updateBlockchainStats(blockchainName, func(stats *BlockchainStats) {
			stats.LastProcessedBlock = blockHeight
			stats.LastUpdateTime = time.Now()
			stats.IsHealthy = true
		})
		return nil
	}

	s.associateEventsWithUsers(events, blockchainName)

	if err := s.eventPublisher.PublishEvents(ctx, events); err != nil {
		s.logger.WithError(err).
			WithField("blockchain", blockchainName).
			WithField("events_count", len(events)).
			Error("Failed to publish events")
		return fmt.Errorf("failed to publish events: %w", err)
	}

	s.updateBlockchainStats(blockchainName, func(stats *BlockchainStats) {
		stats.LastProcessedBlock = blockHeight
		stats.TransactionsFound += int64(len(events))
		stats.EventsPublished += int64(len(events))
		stats.LastUpdateTime = time.Now()
		stats.IsHealthy = true
	})

	s.mu.Lock()
	s.stats.TotalTransactionsFound += int64(len(events))
	s.stats.TotalEventsPublished += int64(len(events))
	s.stats.LastUpdateTime = time.Now()
	s.mu.Unlock()

	processingTime := time.Since(startTime)
	s.logger.WithField("blockchain", blockchainName).
		WithField("block_height", blockHeight).
		WithField("transactions_found", len(events)).
		WithField("processing_time", processingTime).
		Info("Successfully processed block")

	return nil
}

func (s *Service) getMonitoredAddressesForBlockchain(blockchainName string) []string {
	var addresses []string

	for _, userAddresses := range s.config.MonitoredAddresses {
		switch blockchainName {
		case "bitcoin":
			addresses = append(addresses, userAddresses.Bitcoin...)
		case "ethereum":
			addresses = append(addresses, userAddresses.Ethereum...)
		case "solana":
			addresses = append(addresses, userAddresses.Solana...)
		}
	}

	return addresses
}

func (s *Service) associateEventsWithUsers(events []*models.TransactionEvent, blockchainName string) {
	addressToUser := make(map[string]string)

	for userID, userAddresses := range s.config.MonitoredAddresses {
		var addresses []string
		switch blockchainName {
		case "bitcoin":
			addresses = userAddresses.Bitcoin
		case "ethereum":
			addresses = userAddresses.Ethereum
		case "solana":
			addresses = userAddresses.Solana
		}

		for _, addr := range addresses {
			addressToUser[addr] = userID
		}
	}

	for _, event := range events {
		if userID, exists := addressToUser[event.Source]; exists {
			event.UserID = userID
			continue
		}

		if userID, exists := addressToUser[event.Destination]; exists {
			event.UserID = userID
			continue
		}

		event.UserID = "unknown"
	}
}

func (s *Service) extractBlockHeight(block interface{}, blockchainName string) uint64 {
	switch blockchainName {
	case "bitcoin":
		if btcBlock, ok := block.(*blockchain.BitcoinBlock); ok {
			return btcBlock.Height
		}
	case "ethereum":
		if ethBlock, ok := block.(interface {
			Number() interface{ Uint64() uint64 }
		}); ok {
			return ethBlock.Number().Uint64()
		}
	case "solana":
		if solBlock, ok := block.(interface{ ParentSlot() uint64 }); ok {
			return solBlock.ParentSlot() + 1
		}
	}

	return 0
}

func (s *Service) reportStats(ctx context.Context) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.shutdownCh:
			return nil
		case <-ticker.C:
			s.logStats()
		}
	}
}

func (s *Service) logStats() {
	s.mu.RLock()
	stats := s.stats
	s.mu.RUnlock()

	uptime := time.Since(stats.StartTime)

	s.logger.WithFields(logrus.Fields{
		"uptime":                   uptime,
		"total_transactions_found": stats.TotalTransactionsFound,
		"total_events_published":   stats.TotalEventsPublished,
		"total_errors":             stats.TotalErrors,
		"blockchain_count":         len(stats.BlockchainStats),
	}).Info("Service statistics")

	for name, blockchainStats := range stats.BlockchainStats {
		s.logger.WithFields(logrus.Fields{
			"blockchain":           name,
			"last_processed_block": blockchainStats.LastProcessedBlock,
			"current_block":        blockchainStats.CurrentBlock,
			"transactions_found":   blockchainStats.TransactionsFound,
			"events_published":     blockchainStats.EventsPublished,
			"errors":               blockchainStats.Errors,
			"is_healthy":           blockchainStats.IsHealthy,
		}).Debug("Blockchain statistics")
	}
}

func (s *Service) GetStats() ServiceStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}

func (s *Service) IsHealthy(ctx context.Context) bool {
	s.mu.RLock()
	running := s.running
	s.mu.RUnlock()

	if !running {
		return false
	}

	for name, client := range s.clientManager.GetAllClients() {
		if !client.IsHealthy(ctx) {
			s.logger.WithField("blockchain", name).Warn("Blockchain client is unhealthy")
			return false
		}
	}

	if !s.eventPublisher.IsHealthy(ctx) {
		s.logger.Warn("Event publisher is unhealthy")
		return false
	}

	return true
}

func (s *Service) setLastProcessedBlock(blockchainName string, height uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastProcessed[blockchainName] = height
}

func (s *Service) updateBlockchainStats(blockchainName string, updateFunc func(*BlockchainStats)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := s.stats.BlockchainStats[blockchainName]
	updateFunc(&stats)
	s.stats.BlockchainStats[blockchainName] = stats
}

func (s *Service) incrementErrors(blockchainName string) {
	s.updateBlockchainStats(blockchainName, func(stats *BlockchainStats) {
		stats.Errors++
		stats.LastUpdateTime = time.Now()
		stats.IsHealthy = false
	})

	s.mu.Lock()
	s.stats.TotalErrors++
	s.mu.Unlock()
}
