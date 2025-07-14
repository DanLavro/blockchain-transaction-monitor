package blockchain

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"blockchain-monitor/config"
	"blockchain-monitor/models"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/solana-go/rpc/ws"
	"github.com/sirupsen/logrus"
)

type SolanaClient struct {
	config    config.SolanaConfig
	client    *rpc.Client
	wsClient  *ws.Client
	logger    *logrus.Logger
	mu        sync.RWMutex
	connected bool
}

func NewSolanaClient(cfg config.SolanaConfig, logger *logrus.Logger) *SolanaClient {
	return &SolanaClient{
		config: cfg,
		logger: logger,
	}
}

func (sc *SolanaClient) Connect(ctx context.Context) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.client = rpc.New(sc.config.RPCURL)

	if sc.config.WSUrl != "" {
		wsClient, err := ws.Connect(ctx, sc.config.WSUrl)
		if err != nil {
			sc.logger.WithError(err).Warn("Failed to connect to Solana WebSocket, will use polling")
		} else {
			sc.wsClient = wsClient
		}
	}

	_, err := sc.client.GetVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Solana RPC: %w", err)
	}

	sc.connected = true
	sc.logger.WithField("blockchain", "solana").Info("Successfully connected to Solana")
	return nil
}

func (sc *SolanaClient) Disconnect() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.wsClient != nil {
		sc.wsClient.Close()
	}

	sc.connected = false
	sc.logger.WithField("blockchain", "solana").Info("Disconnected from Solana")
	return nil
}

func (sc *SolanaClient) GetLatestBlockHeight(ctx context.Context) (uint64, error) {
	slot, err := sc.client.GetSlot(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest slot: %w", err)
	}
	return slot, nil
}

func (sc *SolanaClient) GetBlock(ctx context.Context, slot uint64) (interface{}, error) {
	out, err := sc.client.GetBlockWithOpts(
		ctx,
		slot,
		&rpc.GetBlockOpts{
			Encoding:                       solana.EncodingJSON,
			TransactionDetails:             rpc.TransactionDetailsFull,
			Rewards:                        &[]bool{true}[0],
			Commitment:                     rpc.CommitmentFinalized,
			MaxSupportedTransactionVersion: &[]uint64{0}[0],
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get block %d: %w", slot, err)
	}
	return out, nil
}

func (sc *SolanaClient) GetTransaction(ctx context.Context, signature string) (interface{}, error) {
	out, err := sc.client.GetTransaction(
		ctx,
		solana.MustSignatureFromBase58(signature),
		&rpc.GetTransactionOpts{
			Encoding:                       solana.EncodingJSON,
			Commitment:                     rpc.CommitmentFinalized,
			MaxSupportedTransactionVersion: &[]uint64{0}[0],
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction %s: %w", signature, err)
	}
	return out, nil
}

func (sc *SolanaClient) SubscribeToBlocks(ctx context.Context, callback func(interface{})) error {
	return sc.subscribeToSlotsPolling(ctx, callback)
}

func (sc *SolanaClient) subscribeToSlotsPolling(ctx context.Context, callback func(interface{})) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastSlot uint64

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			currentSlot, err := sc.GetLatestBlockHeight(ctx)
			if err != nil {
				sc.logger.WithError(err).Error("Failed to get latest slot")
				continue
			}

			if currentSlot > lastSlot {
				for slot := lastSlot + 1; slot <= currentSlot; slot++ {
					block, err := sc.GetBlock(ctx, slot)
					if err != nil {
						sc.logger.WithError(err).WithField("slot", slot).Warn("Failed to get block")
						continue
					}
					callback(block)
				}
				lastSlot = currentSlot
			}
		}
	}
}

func (sc *SolanaClient) ProcessTransactionsInBlock(ctx context.Context, blockInterface interface{}, monitoredAddresses []string) ([]*models.TransactionEvent, error) {
	block, ok := blockInterface.(*rpc.GetBlockResult)
	if !ok {
		return nil, fmt.Errorf("invalid Solana block type")
	}

	if block == nil || block.Transactions == nil {
		return nil, nil
	}

	var events []*models.TransactionEvent
	addressSet := make(map[string]bool)
	for _, addr := range monitoredAddresses {
		addressSet[addr] = true
	}

	for _, tx := range block.Transactions {
		if tx.Meta == nil || tx.Transaction == nil {
			continue
		}

		involvedAddresses := sc.getInvolvedAddresses(tx)
		var relevantAddresses []string

		for _, addr := range involvedAddresses {
			if addressSet[addr] {
				relevantAddresses = append(relevantAddresses, addr)
			}
		}

		if len(relevantAddresses) > 0 {
			signature := "unknown"

			balanceChanges := sc.calculateBalanceChanges(tx, relevantAddresses)

			for addr, change := range balanceChanges {
				var source, destination string
				var amount uint64

				if change > 0 {
					destination = addr
					amount = uint64(change)
				} else if change < 0 {
					source = addr
					amount = uint64(-change)
				} else {
					continue
				}

				event := &models.TransactionEvent{
					EventID:       fmt.Sprintf("sol_%s_%s_%d", signature, addr, time.Now().UnixNano()),
					Timestamp:     sc.getBlockTime(block),
					Blockchain:    "solana",
					TransactionID: signature,
					BlockHeight:   block.ParentSlot + 1, // Approximate block height
					Source:        source,
					Destination:   destination,
					Amount:        strconv.FormatUint(amount, 10),
					Fees:          strconv.FormatUint(tx.Meta.Fee, 10),
					Confirmations: sc.calculateConfirmations(ctx, block.ParentSlot+1),
					Status:        sc.getTransactionStatus(tx.Meta),
					RawData:       map[string]interface{}{"solana_tx": tx},
				}
				events = append(events, event)
			}
		}
	}

	return events, nil
}

func (sc *SolanaClient) GetName() string {
	return "solana"
}

func (sc *SolanaClient) IsHealthy(ctx context.Context) bool {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if !sc.connected || sc.client == nil {
		return false
	}

	_, err := sc.client.GetVersion(ctx)
	return err == nil
}

func (sc *SolanaClient) getInvolvedAddresses(tx rpc.TransactionWithMeta) []string {
	var addresses []string

	fmt.Println(tx) // TODO: Implement this in production

	return addresses
}

func (sc *SolanaClient) calculateBalanceChanges(tx rpc.TransactionWithMeta, monitoredAddresses []string) map[string]int64 {
	changes := make(map[string]int64)

	if tx.Meta == nil {
		return changes
	}

	fmt.Println(monitoredAddresses) // TODO: Implement this in production

	return changes
}

func (sc *SolanaClient) getBlockTime(block *rpc.GetBlockResult) time.Time {
	if block != nil && block.BlockTime != nil {
		return block.BlockTime.Time()
	}
	return time.Now()
}

func (sc *SolanaClient) calculateConfirmations(ctx context.Context, slot uint64) int {
	latest, err := sc.GetLatestBlockHeight(ctx)
	if err != nil {
		return 0
	}
	if latest >= slot {
		return int(latest - slot + 1)
	}
	return 0
}

func (sc *SolanaClient) getTransactionStatus(meta *rpc.TransactionMeta) models.TransactionStatus {
	if meta == nil {
		return models.StatusPending
	}

	if meta.Err != nil {
		return models.StatusFailed
	}

	return models.StatusConfirmed
}
