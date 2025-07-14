package models

import (
	"math/big"
	"time"
)

type TransactionEvent struct {
	EventID       string    `json:"event_id"`
	Timestamp     time.Time `json:"timestamp"`
	Blockchain    string    `json:"blockchain"`
	UserID        string    `json:"user_id"`
	TransactionID string    `json:"transaction_id"`
	BlockHeight   uint64    `json:"block_height"`

	Source      string `json:"source"`
	Destination string `json:"destination"`
	Amount      string `json:"amount"`
	Fees        string `json:"fees"`

	Confirmations int                    `json:"confirmations"`
	Status        TransactionStatus      `json:"status"`
	RawData       map[string]interface{} `json:"raw_data,omitempty"`
}

type TransactionStatus string

const (
	StatusPending   TransactionStatus = "pending"
	StatusConfirmed TransactionStatus = "confirmed"
	StatusFailed    TransactionStatus = "failed"
)

type BitcoinTransaction struct {
	TxID          string          `json:"txid"`
	BlockHash     string          `json:"block_hash"`
	BlockHeight   uint64          `json:"block_height"`
	Timestamp     time.Time       `json:"timestamp"`
	Confirmations int             `json:"confirmations"`
	Inputs        []BitcoinInput  `json:"inputs"`
	Outputs       []BitcoinOutput `json:"outputs"`
	Fees          int64           `json:"fees"`
	Size          int             `json:"size"`
	Weight        int             `json:"weight"`
}

type BitcoinInput struct {
	TxID         string   `json:"txid"`
	Vout         uint32   `json:"vout"`
	ScriptSig    string   `json:"script_sig"`
	Sequence     uint32   `json:"sequence"`
	Witness      []string `json:"witness,omitempty"`
	PrevOutValue int64    `json:"prev_out_value"`
	Address      string   `json:"address,omitempty"`
}

type BitcoinOutput struct {
	Value        int64  `json:"value"`
	N            uint32 `json:"n"`
	ScriptPubKey string `json:"script_pub_key"`
	Address      string `json:"address,omitempty"`
	Type         string `json:"type,omitempty"`
}

type EthereumTransaction struct {
	Hash             string        `json:"hash"`
	BlockHash        string        `json:"block_hash"`
	BlockNumber      uint64        `json:"block_number"`
	TransactionIndex uint64        `json:"transaction_index"`
	Timestamp        time.Time     `json:"timestamp"`
	From             string        `json:"from"`
	To               string        `json:"to,omitempty"`
	Value            *big.Int      `json:"value"`
	Gas              uint64        `json:"gas"`
	GasPrice         *big.Int      `json:"gas_price"`
	GasUsed          uint64        `json:"gas_used"`
	Nonce            uint64        `json:"nonce"`
	Data             string        `json:"data,omitempty"`
	Status           uint64        `json:"status"`
	Logs             []EthereumLog `json:"logs,omitempty"`
}

type EthereumLog struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	BlockNumber uint64   `json:"block_number"`
	TxHash      string   `json:"transaction_hash"`
	TxIndex     uint64   `json:"transaction_index"`
	LogIndex    uint64   `json:"log_index"`
	Removed     bool     `json:"removed"`
}

type SolanaTransaction struct {
	Signature   string                 `json:"signature"`
	Slot        uint64                 `json:"slot"`
	BlockTime   *time.Time             `json:"block_time,omitempty"`
	Meta        *SolanaTransactionMeta `json:"meta,omitempty"`
	Transaction SolanaTransactionData  `json:"transaction"`
}

type SolanaTransactionMeta struct {
	Err               interface{}              `json:"err"`
	Fee               uint64                   `json:"fee"`
	PreBalances       []uint64                 `json:"pre_balances"`
	PostBalances      []uint64                 `json:"post_balances"`
	InnerInstructions []SolanaInnerInstruction `json:"inner_instructions,omitempty"`
	LogMessages       []string                 `json:"log_messages,omitempty"`
	PreTokenBalances  []SolanaTokenBalance     `json:"pre_token_balances,omitempty"`
	PostTokenBalances []SolanaTokenBalance     `json:"post_token_balances,omitempty"`
	Rewards           []SolanaReward           `json:"rewards,omitempty"`
	Status            map[string]interface{}   `json:"status"`
}

type SolanaTransactionData struct {
	Message    SolanaMessage `json:"message"`
	Signatures []string      `json:"signatures"`
}

type SolanaMessage struct {
	AccountKeys     []string            `json:"account_keys"`
	Header          SolanaMessageHeader `json:"header"`
	RecentBlockhash string              `json:"recent_blockhash"`
	Instructions    []SolanaInstruction `json:"instructions"`
}

type SolanaMessageHeader struct {
	NumRequiredSignatures       int `json:"num_required_signatures"`
	NumReadonlySignedAccounts   int `json:"num_readonly_signed_accounts"`
	NumReadonlyUnsignedAccounts int `json:"num_readonly_unsigned_accounts"`
}

type SolanaInstruction struct {
	ProgramIDIndex int    `json:"program_id_index"`
	Accounts       []int  `json:"accounts"`
	Data           string `json:"data"`
}

type SolanaInnerInstruction struct {
	Index        int                 `json:"index"`
	Instructions []SolanaInstruction `json:"instructions"`
}

type SolanaTokenBalance struct {
	AccountIndex  int                 `json:"account_index"`
	Mint          string              `json:"mint"`
	UITokenAmount SolanaUITokenAmount `json:"ui_token_amount"`
	Owner         string              `json:"owner,omitempty"`
	ProgramID     string              `json:"program_id,omitempty"`
}

type SolanaUITokenAmount struct {
	Amount         string  `json:"amount"`
	Decimals       int     `json:"decimals"`
	UIAmount       float64 `json:"ui_amount"`
	UIAmountString string  `json:"ui_amount_string"`
}

type SolanaReward struct {
	Pubkey      string `json:"pubkey"`
	Lamports    int64  `json:"lamports"`
	PostBalance uint64 `json:"post_balance"`
	RewardType  string `json:"reward_type,omitempty"`
	Commission  *int   `json:"commission,omitempty"`
}

type MonitoredAddress struct {
	UserID     string `json:"user_id"`
	Address    string `json:"address"`
	Blockchain string `json:"blockchain"`
	Label      string `json:"label,omitempty"`
}

type BlockchainStatus struct {
	Blockchain         string    `json:"blockchain"`
	LastProcessedBlock uint64    `json:"last_processed_block"`
	CurrentBlock       uint64    `json:"current_block"`
	LastUpdated        time.Time `json:"last_updated"`
	IsHealthy          bool      `json:"is_healthy"`
	ErrorMessage       string    `json:"error_message,omitempty"`
}
