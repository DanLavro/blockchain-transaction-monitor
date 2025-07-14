package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"blockchain-monitor/config"
	"blockchain-monitor/models"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Producer struct {
	writer    *kafka.Writer
	config    config.KafkaConfig
	logger    *logrus.Logger
	mu        sync.RWMutex
	connected bool
}

type ProducerStats struct {
	TotalMessagesSent int64         `json:"total_messages_sent"`
	TotalErrors       int64         `json:"total_errors"`
	LastMessageTime   time.Time     `json:"last_message_time"`
	AverageLatency    time.Duration `json:"average_latency"`
}

func NewProducer(cfg config.KafkaConfig, logger *logrus.Logger) *Producer {
	return &Producer{
		config: cfg,
		logger: logger,
	}
}

func (p *Producer) Connect(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.writer = &kafka.Writer{
		Addr:         kafka.TCP(p.config.Brokers...),
		Topic:        p.config.Topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Compression:  kafka.Snappy,
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			p.logger.WithField("component", "kafka").Errorf(msg, args...)
		}),
	}

	testMessage := kafka.Message{
		Key:   []byte("test"),
		Value: []byte(`{"test": "connection"}`),
		Time:  time.Now(),
	}

	if err := p.writer.WriteMessages(ctx, testMessage); err != nil {
		return fmt.Errorf("failed to connect to Kafka: %w", err)
	}

	p.connected = true
	p.logger.WithField("topic", p.config.Topic).WithField("brokers", p.config.Brokers).Info("Successfully connected to Kafka")
	return nil
}

func (p *Producer) Disconnect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.writer != nil {
		if err := p.writer.Close(); err != nil {
			return fmt.Errorf("failed to close Kafka writer: %w", err)
		}
	}

	p.connected = false
	p.logger.WithField("component", "kafka").Info("Disconnected from Kafka")
	return nil
}

func (p *Producer) PublishEvent(ctx context.Context, event *models.TransactionEvent) error {
	if !p.isConnected() {
		return fmt.Errorf("producer is not connected")
	}

	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	message := kafka.Message{
		Key:   []byte(fmt.Sprintf("%s_%s", event.Blockchain, event.UserID)),
		Value: eventData,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "blockchain", Value: []byte(event.Blockchain)},
			{Key: "user_id", Value: []byte(event.UserID)},
			{Key: "event_type", Value: []byte("transaction")},
			{Key: "transaction_id", Value: []byte(event.TransactionID)},
		},
	}

	if err := p.writer.WriteMessages(ctx, message); err != nil {
		p.logger.WithError(err).WithField("event_id", event.EventID).Error("Failed to publish event")
		return fmt.Errorf("failed to publish event: %w", err)
	}

	p.logger.WithField("event_id", event.EventID).
		WithField("blockchain", event.Blockchain).
		WithField("user_id", event.UserID).
		WithField("transaction_id", event.TransactionID).
		Debug("Successfully published transaction event")

	return nil
}

func (p *Producer) PublishEvents(ctx context.Context, events []*models.TransactionEvent) error {
	if !p.isConnected() {
		return fmt.Errorf("producer is not connected")
	}

	if len(events) == 0 {
		return nil
	}

	messages := make([]kafka.Message, 0, len(events))
	for _, event := range events {
		eventData, err := json.Marshal(event)
		if err != nil {
			p.logger.WithError(err).WithField("event_id", event.EventID).Warn("Failed to marshal event, skipping")
			continue
		}

		message := kafka.Message{
			Key:   []byte(fmt.Sprintf("%s_%s", event.Blockchain, event.UserID)),
			Value: eventData,
			Time:  time.Now(),
			Headers: []kafka.Header{
				{Key: "blockchain", Value: []byte(event.Blockchain)},
				{Key: "user_id", Value: []byte(event.UserID)},
				{Key: "event_type", Value: []byte("transaction")},
				{Key: "transaction_id", Value: []byte(event.TransactionID)},
			},
		}
		messages = append(messages, message)
	}

	if len(messages) == 0 {
		return fmt.Errorf("no valid events to publish")
	}

	if err := p.writer.WriteMessages(ctx, messages...); err != nil {
		p.logger.WithError(err).WithField("count", len(messages)).Error("Failed to publish event batch")
		return fmt.Errorf("failed to publish event batch: %w", err)
	}

	p.logger.WithField("count", len(messages)).
		WithField("events", p.getEventSummary(events)).
		Info("Successfully published transaction event batch")

	return nil
}

func (p *Producer) PublishEventAsync(ctx context.Context, event *models.TransactionEvent, callback func(error)) {
	go func() {
		err := p.PublishEvent(ctx, event)
		if callback != nil {
			callback(err)
		}
	}()
}

func (p *Producer) PublishEventsAsync(ctx context.Context, events []*models.TransactionEvent, callback func(error)) {
	go func() {
		err := p.PublishEvents(ctx, events)
		if callback != nil {
			callback(err)
		}
	}()
}

func (p *Producer) GetStats() ProducerStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.writer == nil {
		return ProducerStats{}
	}

	stats := p.writer.Stats()
	return ProducerStats{
		TotalMessagesSent: stats.Messages,
		TotalErrors:       stats.Errors,
		LastMessageTime:   time.Now(),
		AverageLatency:    0,
	}
}

func (p *Producer) IsHealthy(ctx context.Context) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if !p.connected || p.writer == nil {
		return false
	}

	testCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	testMessage := kafka.Message{
		Key:   []byte("health_check"),
		Value: []byte(`{"type": "health_check", "timestamp": "` + time.Now().Format(time.RFC3339) + `"}`),
		Time:  time.Now(),
	}

	err := p.writer.WriteMessages(testCtx, testMessage)
	return err == nil
}

func (p *Producer) isConnected() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.connected
}

func (p *Producer) getEventSummary(events []*models.TransactionEvent) map[string]int {
	summary := make(map[string]int)
	for _, event := range events {
		summary[event.Blockchain]++
	}
	return summary
}

type EventPublisher interface {
	Connect(ctx context.Context) error
	Disconnect() error
	PublishEvent(ctx context.Context, event *models.TransactionEvent) error
	PublishEvents(ctx context.Context, events []*models.TransactionEvent) error
	PublishEventAsync(ctx context.Context, event *models.TransactionEvent, callback func(error))
	PublishEventsAsync(ctx context.Context, events []*models.TransactionEvent, callback func(error))
	IsHealthy(ctx context.Context) bool
	GetStats() ProducerStats
}

var _ EventPublisher = (*Producer)(nil)
