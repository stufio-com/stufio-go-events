package producer

import (
    "encoding/json"
    "fmt"
    "sync"
    "time"
    "github.com/IBM/sarama"
    "github.com/nameniac/stufio-go-events/config"
    "github.com/nameniac/stufio-go-events/messages"
    "github.com/google/uuid"
)

// EventProducer is responsible for producing event messages to Kafka
type EventProducer struct {
    config   *config.KafkaConfig
    producer sarama.SyncProducer
    mu       sync.Mutex
}

// NewEventProducer creates a new Kafka event producer
func NewEventProducer(config *config.KafkaConfig) (*EventProducer, error) {
    saramaConfig, err := config.CreateProducerConfig()
    if err != nil {
        return nil, fmt.Errorf("creating producer config: %w", err)
    }
    
    producer, err := sarama.NewSyncProducer(config.GetBrokersList(), saramaConfig)
    if err != nil {
        return nil, fmt.Errorf("creating producer: %w", err)
    }
    
    return &EventProducer{
        config:   config,
        producer: producer,
    }, nil
}

// Close closes the producer
func (p *EventProducer) Close() error {
    p.mu.Lock()
    defer p.mu.Unlock()
    
    if p.producer != nil {
        if err := p.producer.Close(); err != nil {
            return fmt.Errorf("closing producer: %w", err)
        }
        p.producer = nil
    }
    
    return nil
}

// Publish sends an event message to Kafka
func (p *EventProducer) Publish(topic string, eventMsg *messages.BaseEventMessage) error {
    p.mu.Lock()
    defer p.mu.Unlock()
    
    if p.producer == nil {
        return fmt.Errorf("producer is closed")
    }
    
    // Ensure required fields
    if eventMsg.EventID == "" {
        eventMsg.EventID = uuid.New().String()
    }
    
    if eventMsg.Timestamp.IsZero() {
        eventMsg.Timestamp = time.Now()
    }
    
    // Serialize the message
    value, err := json.Marshal(eventMsg)
    if err != nil {
        return fmt.Errorf("marshalling event: %w", err)
    }
    
    // Create message headers
    headers := []sarama.RecordHeader{
        {
            Key:   []byte("entity_type"),
            Value: []byte(eventMsg.Entity.Type),
        },
        {
            Key:   []byte("action"),
            Value: []byte(eventMsg.Action),
        },
        {
            Key:   []byte("event_id"),
            Value: []byte(eventMsg.EventID),
        },
    }
    
    // Add correlation ID header if present
    if eventMsg.CorrelationID != nil {
        headers = append(headers, sarama.RecordHeader{
            Key:   []byte("correlation_id"),
            Value: []byte(*eventMsg.CorrelationID),
        })
    }
    
    // Create and send the message
    msg := &sarama.ProducerMessage{
        Topic:   topic,
        Value:   sarama.ByteEncoder(value),
        Headers: headers,
        Key:     sarama.StringEncoder(eventMsg.Entity.ID), // Use entity ID as key for partitioning
    }
    
    _, _, err = p.producer.SendMessage(msg)
    if err != nil {
        return fmt.Errorf("sending message: %w", err)
    }
    
    return nil
}

// PublishTyped sends a typed event message to Kafka
func (p *EventProducer) PublishTyped[T any](topic string, eventMsg *messages.TypedEventMessage[T]) error {
    // Convert to base event message
    baseMsg := &messages.BaseEventMessage{
        EventID:       eventMsg.EventID,
        CorrelationID: eventMsg.CorrelationID,
        Timestamp:     eventMsg.Timestamp,
        Entity:        eventMsg.Entity,
        Action:        eventMsg.Action,
        Actor:         eventMsg.Actor,
        Payload:       eventMsg.Payload,
        Metrics:       eventMsg.Metrics,
    }
    
    return p.Publish(topic, baseMsg)
}

// PublishEvent sends an event using the registered event definition
func (p *EventProducer) PublishEvent(eventDef messages.EventDefinition, entityID string, actorType messages.ActorType, 
                                   actorID string, payload interface{}) error {
    // Create event message
    eventMsg := &messages.BaseEventMessage{
        EventID:   uuid.New().String(),
        Timestamp: time.Now(),
        Entity: messages.Entity{
            Type: eventDef.EntityType,
            ID:   entityID,
        },
        Action: eventDef.Action,
        Actor: messages.Actor{
            Type: actorType,
            ID:   actorID,
        },
        Payload: payload,
    }
    
    return p.Publish(eventDef.Topic, eventMsg)
}