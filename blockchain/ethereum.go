package blockchain

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"blockchain-monitor/config"
	"blockchain-monitor/models"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/sirupsen/logrus"
)

type EthereumClient struct {
	config    config.EthereumConfig
	client    *ethclient.Client
	wsClient  *ethclient.Client
	logger    *logrus.Logger
	mu        sync.RWMutex
	connected bool
}

func NewEthereumClient(cfg config.EthereumConfig, logger *logrus.Logger) *EthereumClient {
	return &EthereumClient{
		config: cfg,
		logger: logger,
	}
}

func (ec *EthereumClient) Connect(ctx context.Context) error {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	client, err := ethclient.DialContext(ctx, ec.config.RPCURL)
	if err != nil {
		return fmt.Errorf("failed to connect to Ethereum RPC: %w", err)
	}
	ec.client = client

	if ec.config.WSUrl != "" {
		wsClient, err := ethclient.DialContext(ctx, ec.config.WSUrl)
		if err != nil {
			ec.logger.WithError(err).Warn("Failed to connect to Ethereum WebSocket, will use polling")
		} else {
			ec.wsClient = wsClient
		}
	}

	chainID, err := ec.client.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("failed to get chain ID: %w", err)
	}

	if chainID.Int64() != ec.config.ChainID {
		return fmt.Errorf("chain ID mismatch: expected %d, got %d", ec.config.ChainID, chainID.Int64())
	}

	ec.connected = true
	ec.logger.WithField("blockchain", "ethereum").WithField("chain_id", chainID.Int64()).Info("Successfully connected to Ethereum")
	return nil
}

func (ec *EthereumClient) Disconnect() error {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	if ec.client != nil {
		ec.client.Close()
	}
	if ec.wsClient != nil {
		ec.wsClient.Close()
	}

	ec.connected = false
	ec.logger.WithField("blockchain", "ethereum").Info("Disconnected from Ethereum")
	return nil
}

func (ec *EthereumClient) GetLatestBlockHeight(ctx context.Context) (uint64, error) {
	header, err := ec.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest block header: %w", err)
	}
	return header.Number.Uint64(), nil
}

func (ec *EthereumClient) GetBlock(ctx context.Context, height uint64) (interface{}, error) {
	block, err := ec.client.BlockByNumber(ctx, big.NewInt(int64(height)))
	if err != nil {
		return nil, fmt.Errorf("failed to get block %d: %w", height, err)
	}
	return block, nil
}

func (ec *EthereumClient) GetTransaction(ctx context.Context, txID string) (interface{}, error) {
	hash := common.HexToHash(txID)

	tx, isPending, err := ec.client.TransactionByHash(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction %s: %w", txID, err)
	}

	var receipt *types.Receipt
	if !isPending {
		receipt, err = ec.client.TransactionReceipt(ctx, hash)
		if err != nil {
			ec.logger.WithError(err).WithField("txid", txID).Warn("Failed to get transaction receipt")
		}
	}

	ethTx := &models.EthereumTransaction{
		Hash:     tx.Hash().Hex(),
		From:     ec.getFromAddress(tx),
		To:       ec.getToAddress(tx),
		Value:    tx.Value(),
		Gas:      tx.Gas(),
		GasPrice: tx.GasPrice(),
		Nonce:    tx.Nonce(),
		Data:     common.Bytes2Hex(tx.Data()),
	}

	if receipt != nil {
		ethTx.BlockHash = receipt.BlockHash.Hex()
		ethTx.BlockNumber = receipt.BlockNumber.Uint64()
		ethTx.TransactionIndex = uint64(receipt.TransactionIndex)
		ethTx.GasUsed = receipt.GasUsed
		ethTx.Status = uint64(receipt.Status)

		for _, log := range receipt.Logs {
			ethLog := models.EthereumLog{
				Address:     log.Address.Hex(),
				Data:        common.Bytes2Hex(log.Data),
				BlockNumber: log.BlockNumber,
				TxHash:      log.TxHash.Hex(),
				TxIndex:     uint64(log.TxIndex),
				LogIndex:    uint64(log.Index),
				Removed:     log.Removed,
			}

			for _, topic := range log.Topics {
				ethLog.Topics = append(ethLog.Topics, topic.Hex())
			}

			ethTx.Logs = append(ethTx.Logs, ethLog)
		}
	}

	return ethTx, nil
}

func (ec *EthereumClient) SubscribeToBlocks(ctx context.Context, callback func(interface{})) error {
	if ec.wsClient != nil {
		return ec.subscribeToBlocksWS(ctx, callback)
	} else {
		return ec.subscribeToBlocksPolling(ctx, callback)
	}
}

func (ec *EthereumClient) subscribeToBlocksWS(ctx context.Context, callback func(interface{})) error {
	headers := make(chan *types.Header)
	subscription, err := ec.wsClient.SubscribeNewHead(ctx, headers)
	if err != nil {
		return fmt.Errorf("failed to subscribe to new blocks: %w", err)
	}
	defer subscription.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-subscription.Err():
			return fmt.Errorf("subscription error: %w", err)
		case header := <-headers:
			block, err := ec.client.BlockByHash(ctx, header.Hash())
			if err != nil {
				ec.logger.WithError(err).WithField("hash", header.Hash().Hex()).Error("Failed to get block")
				continue
			}
			callback(block)
		}
	}
}

func (ec *EthereumClient) subscribeToBlocksPolling(ctx context.Context, callback func(interface{})) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastHeight uint64

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			currentHeight, err := ec.GetLatestBlockHeight(ctx)
			if err != nil {
				ec.logger.WithError(err).Error("Failed to get latest block height")
				continue
			}

			if currentHeight > lastHeight {
				for height := lastHeight + 1; height <= currentHeight; height++ {
					block, err := ec.GetBlock(ctx, height)
					if err != nil {
						ec.logger.WithError(err).WithField("height", height).Error("Failed to get block")
						continue
					}
					callback(block)
				}
				lastHeight = currentHeight
			}
		}
	}
}

func (ec *EthereumClient) ProcessTransactionsInBlock(ctx context.Context, blockInterface interface{}, monitoredAddresses []string) ([]*models.TransactionEvent, error) {
	block, ok := blockInterface.(*types.Block)
	if !ok {
		return nil, fmt.Errorf("invalid Ethereum block type")
	}

	var events []*models.TransactionEvent
	addressSet := make(map[string]bool)
	for _, addr := range monitoredAddresses {
		addressSet[strings.ToLower(addr)] = true
	}

	for _, tx := range block.Transactions() {
		fromAddr := ec.getFromAddress(tx)
		toAddr := ec.getToAddress(tx)

		var relevantAddr string
		var isIncoming bool

		if addressSet[strings.ToLower(fromAddr)] {
			relevantAddr = fromAddr
			isIncoming = false
		} else if toAddr != "" && addressSet[strings.ToLower(toAddr)] {
			relevantAddr = toAddr
			isIncoming = true
		}

		if relevantAddr != "" {
			receipt, err := ec.client.TransactionReceipt(ctx, tx.Hash())
			if err != nil {
				ec.logger.WithError(err).WithField("txid", tx.Hash().Hex()).Warn("Failed to get transaction receipt")
				continue
			}

			gasUsed := big.NewInt(int64(receipt.GasUsed))
			fees := new(big.Int).Mul(gasUsed, tx.GasPrice())

			var source, destination string
			if isIncoming {
				source = fromAddr
				destination = relevantAddr
			} else {
				source = relevantAddr
				destination = toAddr
			}

			event := &models.TransactionEvent{
				EventID:       fmt.Sprintf("eth_%s_%d", tx.Hash().Hex(), time.Now().UnixNano()),
				Timestamp:     time.Unix(int64(block.Time()), 0),
				Blockchain:    "ethereum",
				TransactionID: tx.Hash().Hex(),
				BlockHeight:   block.Number().Uint64(),
				Source:        source,
				Destination:   destination,
				Amount:        tx.Value().String(),
				Fees:          fees.String(),
				Confirmations: ec.calculateConfirmations(ctx, block.Number().Uint64()),
				Status:        ec.getTransactionStatus(receipt.Status),
				RawData:       map[string]interface{}{"ethereum_tx": tx, "receipt": receipt},
			}
			events = append(events, event)
		}
	}

	return events, nil
}

func (ec *EthereumClient) GetName() string {
	return "ethereum"
}

func (ec *EthereumClient) IsHealthy(ctx context.Context) bool {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	if !ec.connected || ec.client == nil {
		return false
	}

	_, err := ec.client.ChainID(ctx)
	return err == nil
}

func (ec *EthereumClient) getFromAddress(tx *types.Transaction) string {
	signer := types.LatestSignerForChainID(big.NewInt(ec.config.ChainID))
	from, err := types.Sender(signer, tx)
	if err != nil {
		ec.logger.WithError(err).Warn("Failed to get transaction sender")
		return ""
	}
	return from.Hex()
}

func (ec *EthereumClient) getToAddress(tx *types.Transaction) string {
	if tx.To() == nil {
		return "" // Contract creation
	}
	return tx.To().Hex()
}

func (ec *EthereumClient) calculateConfirmations(ctx context.Context, blockHeight uint64) int {
	latest, err := ec.GetLatestBlockHeight(ctx)
	if err != nil {
		return 0
	}
	if latest >= blockHeight {
		return int(latest - blockHeight + 1)
	}
	return 0
}

func (ec *EthereumClient) getTransactionStatus(status uint64) models.TransactionStatus {
	if status == 1 {
		return models.StatusConfirmed
	}
	return models.StatusFailed
}
