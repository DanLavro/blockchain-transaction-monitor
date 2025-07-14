# Blockchain Transaction Monitor

A scalable microservice written in Go that monitors Bitcoin, Ethereum, and Solana blockchains for transactions involving specified addresses. The service connects via RPC to blockchain nodes, processes transactions in real-time, and publishes events to Kafka.

## Features

- **Multi-blockchain Support**: Monitor Bitcoin, Ethereum, and Solana simultaneously
- **Real-time Processing**: Handle high-throughput blockchains like Solana efficiently
- **Scalable Architecture**: Built with concurrent processing and proper error handling
- **Event Publishing**: Publishes transaction events to Kafka for downstream processing
- **Configurable**: Flexible configuration via environment variables
- **Health Monitoring**: Built-in health checks and statistics reporting
- **Graceful Shutdown**: Proper cleanup and graceful shutdown handling

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Bitcoin RPC   │    │  Ethereum RPC   │    │   Solana RPC    │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                    ┌─────────────▼──────────────┐
                    │   Blockchain Clients       │
                    │   - Bitcoin Client         │
                    │   - Ethereum Client        │
                    │   - Solana Client          │
                    └─────────────┬──────────────┘
                                 │
                    ┌─────────────▼──────────────┐
                    │   Transaction Monitor      │
                    │   - Block Processing       │
                    │   - Address Filtering      │
                    │   - Event Generation       │
                    └─────────────┬──────────────┘
                                 │
                    ┌─────────────▼──────────────┐
                    │   Kafka Producer           │
                    │   - Event Publishing       │
                    │   - Batching & Compression │
                    └────────────────────────────┘
```

## Transaction Event Schema

Each transaction event published to Kafka contains:

```json
{
  "event_id": "eth_0x1234..._%d",
  "timestamp": "2024-01-15T10:30:00Z",
  "blockchain": "ethereum",
  "user_id": "user123",
  "transaction_id": "0x1234567890abcdef...",
  "block_height": 18500000,
  "source": "0xabc123...",
  "destination": "0xdef456...",
  "amount": "1000000000000000000",
  "fees": "21000000000000000",
  "confirmations": 12,
  "status": "confirmed",
  "raw_data": { /* blockchain-specific data */ }
}
```

## Quick Start

### Prerequisites

- Go 1.21 or higher
- Docker and Docker Compose
- Access to blockchain RPC endpoints (Blockdaemon, Infura, etc.)

### 1. Clone and Setup

```bash
git clone <repository-url>
cd blockchain-monitor
go mod download
```

### 2. Start Kafka Infrastructure

```bash
# Start Kafka and Zookeeper
docker-compose up -d

# Verify Kafka is running
docker-compose ps

# View Kafka UI at http://localhost:8080
```

### 3. Configure Environment

Create a `.env` file:

```bash
# Blockchain RPC Configuration
BITCOIN_RPC_URL=https://bitcoin-mainnet.blockdaemon.com/rpc
BITCOIN_RPC_USERNAME=your_username
BITCOIN_RPC_PASSWORD=your_password

ETHEREUM_RPC_URL=https://ethereum-mainnet.blockdaemon.com/rpc
ETHEREUM_WS_URL=wss://ethereum-mainnet.blockdaemon.com/ws
ETHEREUM_API_KEY=your_api_key

SOLANA_RPC_URL=https://solana-mainnet.blockdaemon.com/rpc
SOLANA_WS_URL=wss://solana-mainnet.blockdaemon.com/ws

# Kafka Configuration
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=blockchain-transactions

# Monitored Addresses (JSON format)
MONITORED_ADDRESSES={"user1":{"bitcoin":["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"],"ethereum":["0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"],"solana":["11111111111111111111111111111112"]}}
```

### 4. Run the Service

```bash
# Build and run
go build -o blockchain-monitor
./blockchain-monitor

# Or run directly
go run main.go
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `BITCOIN_RPC_URL` | Bitcoin RPC endpoint | `https://bitcoin-mainnet.blockdaemon.com/rpc` |
| `BITCOIN_RPC_USERNAME` | Bitcoin RPC username | - |
| `BITCOIN_RPC_PASSWORD` | Bitcoin RPC password | - |
| `ETHEREUM_RPC_URL` | Ethereum RPC endpoint | `https://ethereum-mainnet.blockdaemon.com/rpc` |
| `ETHEREUM_WS_URL` | Ethereum WebSocket endpoint | `wss://ethereum-mainnet.blockdaemon.com/ws` |
| `ETHEREUM_API_KEY` | Ethereum API key | - |
| `SOLANA_RPC_URL` | Solana RPC endpoint | `https://solana-mainnet.blockdaemon.com/rpc` |
| `SOLANA_WS_URL` | Solana WebSocket endpoint | `wss://solana-mainnet.blockdaemon.com/ws` |
| `KAFKA_BROKERS` | Kafka broker addresses | `localhost:9092` |
| `KAFKA_TOPIC` | Kafka topic for events | `blockchain-transactions` |
| `MONITORED_ADDRESSES` | JSON string of addresses to monitor | See example |
| `LOG_LEVEL` | Logging level (debug, info, warn, error) | `info` |
| `WORKER_POOL_SIZE` | Number of worker goroutines | `10` |
| `SHUTDOWN_TIMEOUT` | Graceful shutdown timeout | `30s` |

### Monitored Addresses Format

The `MONITORED_ADDRESSES` environment variable should be a JSON string mapping user IDs to their addresses:

```json
{
  "user1": {
    "bitcoin": ["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"],
    "ethereum": ["0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"],
    "solana": ["11111111111111111111111111111112"]
  },
  "user2": {
    "bitcoin": ["1234567890abcdef..."],
    "ethereum": ["0xabcdef123456..."],
    "solana": ["SolanaAddress123..."]
  }
}
```
