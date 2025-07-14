package blockchain

import (
	"blockchain-monitor/models"
	"context"
	"fmt"
)

type BlockchainClient interface {
	Connect(ctx context.Context) error

	Disconnect() error

	GetLatestBlockHeight(ctx context.Context) (uint64, error)

	GetBlock(ctx context.Context, height uint64) (interface{}, error)

	GetTransaction(ctx context.Context, txID string) (interface{}, error)

	SubscribeToBlocks(ctx context.Context, callback func(interface{})) error

	ProcessTransactionsInBlock(ctx context.Context, block interface{}, monitoredAddresses []string) ([]*models.TransactionEvent, error)

	GetName() string

	IsHealthy(ctx context.Context) bool
}

type ClientManager struct {
	clients map[string]BlockchainClient
}

func NewClientManager() *ClientManager {
	return &ClientManager{
		clients: make(map[string]BlockchainClient),
	}
}

func (cm *ClientManager) AddClient(name string, client BlockchainClient) {
	cm.clients[name] = client
}

func (cm *ClientManager) GetClient(name string) (BlockchainClient, bool) {
	client, exists := cm.clients[name]
	return client, exists
}

func (cm *ClientManager) GetAllClients() map[string]BlockchainClient {
	return cm.clients
}

func (cm *ClientManager) ConnectAll(ctx context.Context) error {
	for name, client := range cm.clients {
		if err := client.Connect(ctx); err != nil {
			return fmt.Errorf("failed to connect to %s: %w", name, err)
		}
	}
	return nil
}

func (cm *ClientManager) DisconnectAll() error {
	var lastErr error
	for name, client := range cm.clients {
		if err := client.Disconnect(); err != nil {
			lastErr = fmt.Errorf("failed to disconnect from %s: %w", name, err)
		}
	}
	return lastErr
}
