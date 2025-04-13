package goevents

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/stufio-com/stufio-go-events/config"
	"github.com/stufio-com/stufio-go-events/consumer"
	"github.com/stufio-com/stufio-go-events/messages"
	"github.com/stufio-com/stufio-go-events/producer"
)

// EventClient provides a unified client for producing and consuming events
type EventClient struct {
	Producer *producer.EventProducer
	Consumer *consumer.EventConsumer
	Config   *config.KafkaConfig
}

// NewEventClient creates a new event client
func NewEventClient(cfg *config.KafkaConfig) (*EventClient, error) {
	// Create producer
	prod, err := producer.NewEventProducer(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating producer: %w", err)
	}

	// Create consumer
	cons, err := consumer.NewEventConsumer(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating consumer: %w", err)
	}

	return &EventClient{
		Producer: prod,
		Consumer: cons,
		Config:   cfg,
	}, nil
}

// Event represents an event in the system
type Event struct {
	EventID       string
	CorrelationID *string
	Entity        Entity
	Action        string
	Actor         Actor
	Payload       interface{}
	Timestamp     time.Time
}

// Entity represents an entity in the system
type Entity struct {
	Type string
	ID   string
}

// Actor represents an actor in the system
type Actor struct {
	Type string
	ID   string
}

// PublishEvent is a convenience method for publishing events
func (c *EventClient) PublishEvent(
	topic string,
	entity Entity,
	action string,
	actor Actor,
	payload interface{},
	eventID string,
) error {
	if eventID == "" {
		eventID = uuid.New().String()
	}

	// Create base event message
	eventMsg := &messages.BaseEventMessage{
		EventID:   eventID,
		Timestamp: time.Now().UTC(),
		Entity: messages.Entity{
			Type: entity.Type,
			ID:   entity.ID,
		},
		Action: action,
		Actor: messages.Actor{
			Type: messages.ActorType(actor.Type),
			ID:   actor.ID,
		},
		Payload: payload,
	}

	return c.Producer.Publish(topic, eventMsg)
}

// RegisterHandler registers a handler for a specific event type
func (c *EventClient) RegisterHandler(entityType string, action string, handler func(context.Context, *Event) error) {
	c.Consumer.RegisterHandler(entityType, action, consumer.NewBasicHandler(
		func(ctx context.Context, msg *messages.BaseEventMessage) error {
			event := &Event{
				EventID:       msg.EventID,
				CorrelationID: msg.CorrelationID,
				Entity: Entity{
					Type: msg.Entity.Type,
					ID:   msg.Entity.ID,
				},
				Action: msg.Action,
				Actor: Actor{
					Type: string(msg.Actor.Type),
					ID:   msg.Actor.ID,
				},
				Payload:   msg.Payload,
				Timestamp: msg.Timestamp,
			}
			return handler(ctx, event)
		},
	))
}

// RegisterTopicHandler registers a handler for all events on a topic
func (c *EventClient) RegisterTopicHandler(topic string, handler func(context.Context, *Event) error) {
	c.Consumer.RegisterTopicHandler(topic, consumer.NewBasicHandler(
		func(ctx context.Context, msg *messages.BaseEventMessage) error {
			event := &Event{
				EventID:       msg.EventID,
				CorrelationID: msg.CorrelationID,
				Entity: Entity{
					Type: msg.Entity.Type,
					ID:   msg.Entity.ID,
				},
				Action: msg.Action,
				Actor: Actor{
					Type: string(msg.Actor.Type),
					ID:   msg.Actor.ID,
				},
				Payload:   msg.Payload,
				Timestamp: msg.Timestamp,
			}
			return handler(ctx, event)
		},
	))
}

// Start starts the consumer
func (c *EventClient) Start(ctx context.Context) error {
	return c.Consumer.Start(ctx)
}

// Close closes both producer and consumer
func (c *EventClient) Close() error {
	var prodErr, consErr error

	if c.Producer != nil {
		prodErr = c.Producer.Close()
	}

	if c.Consumer != nil {
		consErr = c.Consumer.Stop()
	}

	if prodErr != nil {
		return fmt.Errorf("closing producer: %w", prodErr)
	}

	if consErr != nil {
		return fmt.Errorf("closing consumer: %w", consErr)
	}

	return nil
}
