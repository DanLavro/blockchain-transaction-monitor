package blockchain

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"blockchain-monitor/config"
	"blockchain-monitor/models"

	"github.com/sirupsen/logrus"
)

type BitcoinClient struct {
	config     config.BitcoinConfig
	httpClient *http.Client
	logger     *logrus.Logger
	mu         sync.RWMutex
	connected  bool
}

type BitcoinRPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type BitcoinRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      int         `json:"id"`
	Result  interface{} `json:"result"`
	Error   *RPCError   `json:"error"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type BitcoinBlock struct {
	Hash              string   `json:"hash"`
	Height            uint64   `json:"height"`
	Version           int      `json:"version"`
	VersionHex        string   `json:"versionHex"`
	MerkleRoot        string   `json:"merkleroot"`
	Time              int64    `json:"time"`
	MedianTime        int64    `json:"mediantime"`
	Nonce             uint64   `json:"nonce"`
	Bits              string   `json:"bits"`
	Difficulty        float64  `json:"difficulty"`
	Chainwork         string   `json:"chainwork"`
	NTx               int      `json:"nTx"`
	PreviousBlockHash string   `json:"previousblockhash,omitempty"`
	NextBlockHash     string   `json:"nextblockhash,omitempty"`
	Tx                []string `json:"tx"`
	Size              int      `json:"size"`
	Weight            int      `json:"weight"`
	Confirmations     int      `json:"confirmations"`
}

func NewBitcoinClient(cfg config.BitcoinConfig, logger *logrus.Logger) *BitcoinClient {
	return &BitcoinClient{
		config: cfg,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: logger,
	}
}

func (bc *BitcoinClient) Connect(ctx context.Context) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if _, err := bc.rpcCall(ctx, "getblockchaininfo", []interface{}{}); err != nil {
		return fmt.Errorf("failed to connect to Bitcoin RPC: %w", err)
	}

	bc.connected = true
	bc.logger.WithField("blockchain", "bitcoin").Info("Successfully connected to Bitcoin RPC")
	return nil
}

func (bc *BitcoinClient) Disconnect() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	bc.connected = false
	bc.logger.WithField("blockchain", "bitcoin").Info("Disconnected from Bitcoin RPC")
	return nil
}

func (bc *BitcoinClient) GetLatestBlockHeight(ctx context.Context) (uint64, error) {
	result, err := bc.rpcCall(ctx, "getblockcount", []interface{}{})
	if err != nil {
		return 0, fmt.Errorf("failed to get block count: %w", err)
	}

	height, ok := result.(float64)
	if !ok {
		return 0, fmt.Errorf("invalid block height format")
	}

	return uint64(height), nil
}

func (bc *BitcoinClient) GetBlock(ctx context.Context, height uint64) (interface{}, error) {
	hashResult, err := bc.rpcCall(ctx, "getblockhash", []interface{}{height})
	if err != nil {
		return nil, fmt.Errorf("failed to get block hash for height %d: %w", height, err)
	}

	blockHash, ok := hashResult.(string)
	if !ok {
		return nil, fmt.Errorf("invalid block hash format")
	}

	blockResult, err := bc.rpcCall(ctx, "getblock", []interface{}{blockHash, 2})
	if err != nil {
		return nil, fmt.Errorf("failed to get block %s: %w", blockHash, err)
	}

	blockData, err := json.Marshal(blockResult)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal block data: %w", err)
	}

	var block BitcoinBlock
	if err := json.Unmarshal(blockData, &block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block data: %w", err)
	}

	return &block, nil
}

func (bc *BitcoinClient) GetTransaction(ctx context.Context, txID string) (interface{}, error) {
	result, err := bc.rpcCall(ctx, "getrawtransaction", []interface{}{txID, true})
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction %s: %w", txID, err)
	}

	txData, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal transaction data: %w", err)
	}

	var tx models.BitcoinTransaction
	if err := json.Unmarshal(txData, &tx); err != nil {
		return nil, fmt.Errorf("failed to unmarshal transaction data: %w", err)
	}

	return &tx, nil
}

func (bc *BitcoinClient) SubscribeToBlocks(ctx context.Context, callback func(interface{})) error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var lastHeight uint64

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			currentHeight, err := bc.GetLatestBlockHeight(ctx)
			if err != nil {
				bc.logger.WithError(err).Error("Failed to get latest block height")
				continue
			}

			if currentHeight > lastHeight {
				for height := lastHeight + 1; height <= currentHeight; height++ {
					block, err := bc.GetBlock(ctx, height)
					if err != nil {
						bc.logger.WithError(err).WithField("height", height).Error("Failed to get block")
						continue
					}
					callback(block)
				}
				lastHeight = currentHeight
			}
		}
	}
}

func (bc *BitcoinClient) ProcessTransactionsInBlock(ctx context.Context, blockInterface interface{}, monitoredAddresses []string) ([]*models.TransactionEvent, error) {
	block, ok := blockInterface.(*BitcoinBlock)
	if !ok {
		return nil, fmt.Errorf("invalid Bitcoin block type")
	}

	var events []*models.TransactionEvent
	addressSet := make(map[string]bool)
	for _, addr := range monitoredAddresses {
		addressSet[addr] = true
	}

	for _, txID := range block.Tx {
		tx, err := bc.GetTransaction(ctx, txID)
		if err != nil {
			bc.logger.WithError(err).WithField("txid", txID).Warn("Failed to get transaction")
			continue
		}

		btcTx, ok := tx.(*models.BitcoinTransaction)
		if !ok {
			continue
		}

		var source, destination string
		var amount int64

		for _, output := range btcTx.Outputs {
			if output.Address != "" && addressSet[output.Address] {
				if destination == "" {
					destination = output.Address
					amount = output.Value
				}
			}
		}

		for _, input := range btcTx.Inputs {
			if input.Address != "" && addressSet[input.Address] {
				if source == "" {
					source = input.Address
				}
			}
		}

		if source != "" || destination != "" {
			event := &models.TransactionEvent{
				EventID:       fmt.Sprintf("btc_%s_%d", btcTx.TxID, time.Now().UnixNano()),
				Timestamp:     btcTx.Timestamp,
				Blockchain:    "bitcoin",
				TransactionID: btcTx.TxID,
				BlockHeight:   btcTx.BlockHeight,
				Source:        source,
				Destination:   destination,
				Amount:        strconv.FormatInt(amount, 10),
				Fees:          strconv.FormatInt(btcTx.Fees, 10),
				Confirmations: btcTx.Confirmations,
				Status:        bc.getTransactionStatus(btcTx.Confirmations),
				RawData:       map[string]interface{}{"bitcoin_tx": btcTx},
			}
			events = append(events, event)
		}
	}

	return events, nil
}

func (bc *BitcoinClient) GetName() string {
	return "bitcoin"
}

func (bc *BitcoinClient) IsHealthy(ctx context.Context) bool {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	if !bc.connected {
		return false
	}

	_, err := bc.rpcCall(ctx, "getblockchaininfo", []interface{}{})
	return err == nil
}

func (bc *BitcoinClient) rpcCall(ctx context.Context, method string, params []interface{}) (interface{}, error) {
	request := BitcoinRPCRequest{
		JSONRPC: "1.0",
		ID:      1,
		Method:  method,
		Params:  params,
	}

	requestData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", bc.config.RPCURL, bytes.NewBuffer(requestData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if bc.config.Username != "" && bc.config.Password != "" {
		req.SetBasicAuth(bc.config.Username, bc.config.Password)
	}

	resp, err := bc.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(body))
	}

	var response BitcoinRPCResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if response.Error != nil {
		return nil, fmt.Errorf("RPC error %d: %s", response.Error.Code, response.Error.Message)
	}

	return response.Result, nil
}

func (bc *BitcoinClient) getTransactionStatus(confirmations int) models.TransactionStatus {
	if confirmations == 0 {
		return models.StatusPending
	} else if confirmations >= 6 {
		return models.StatusConfirmed
	}
	return models.StatusPending
}
