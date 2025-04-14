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
	"github.com/stufio-com/stufio-go-events/schema"
)

// EventClient provides a high-level interface to the events system
type EventClient struct {
	producer       *producer.EventProducer
	consumer       *consumer.EventConsumer
	config         *config.EventsConfig
	schemaRegistry *schema.SchemaRegistry
}

// NewEventClient creates a new event client
func NewEventClient(kafkaConfig config.KafkaConfig) (*EventClient, error) { // Accept value type for KafkaConfig
	// Create default events config
	eventsConfig := config.DefaultEventsConfig()
	eventsConfig.Kafka = kafkaConfig // Assign the provided Kafka config

	// Pass the address of eventsConfig
	return NewEventClientWithConfig(&eventsConfig) // <-- Pass pointer using &
}

// NewEventClientWithConfig creates an event client with a specified config
func NewEventClientWithConfig(config *config.EventsConfig) (*EventClient, error) { // Expects pointer
	// Initialize schema registry
	// Use config fields directly
	schema.InitGlobalRegistry(config.AsyncAPIURL, config.RefreshInterval, config.SchemaValidation)
	schemaRegistry := schema.GetGlobalRegistry()

	// Create producer - Pass the KafkaConfig value from the main config
	prod, err := producer.NewEventProducer(&config.Kafka)
	if err != nil {
		return nil, fmt.Errorf("creating event producer: %w", err)
	}

	// Create consumer if topics are specified - Pass the KafkaConfig value
	var cons *consumer.EventConsumer
	if len(config.Kafka.Topics) > 0 {
		cons, err = consumer.NewEventConsumer(&config.Kafka)
		if err != nil {
			prod.Close() // Clean up producer if consumer creation fails
			return nil, fmt.Errorf("creating event consumer: %w", err)
		}
	}

	return &EventClient{
		producer:       prod,
		consumer:       cons,
		config:         config, // Store the pointer
		schemaRegistry: schemaRegistry,
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

// PublishEvent publishes an event
func (c *EventClient) PublishEvent(
	topicName string,
	entity Entity,
	action string,
	actor Actor,
	payload interface{},
	correlationID string,
) error {
	// Skip validation when disabled
	if c.config.SchemaValidation {
		if err := c.schemaRegistry.ValidatePayload(entity.Type, action, payload); err != nil {
			// Consider logging the validation error but not returning it,
			// depending on whether validation failure should block publishing.
			// For now, return the error.
			return fmt.Errorf("payload validation failed: %w", err)
		}
	}

	// Create base event message
	eventID := uuid.New().String()
	correlID := &correlationID
	if correlationID == "" {
		correlID = nil
	}

	eventMsg := &messages.BaseEventMessage{
		EventID:       eventID,
		CorrelationID: correlID,
		Timestamp:     time.Now().UTC(), // Use UTC time
		Entity: messages.Entity{
			Type: entity.Type,
			ID:   entity.ID,
		},
		Action: action,
		Actor: messages.Actor{
			Type: messages.ActorType(actor.Type), // Ensure conversion to ActorType
			ID:   actor.ID,
		},
		Payload: payload,
	}

	// --- Determine Topic ---
	topic := topicName
	if topic == "" {
		// Try getting topic from schema registry first
		if eventDef, found := c.schemaRegistry.GetEventDefinition(entity.Type, action); found && eventDef.Topic != "" {
			topic = eventDef.Topic
		} else {
			// Fallback to default topic prefix from config if schema registry has no info
			// or if validation is disabled (schema registry might be empty)
			topic = c.config.KafkaTopicPrefix // <-- Use prefix from config
			if topic == "" {
				// Final fallback if prefix is also empty in config (should not happen with defaults)
				return fmt.Errorf("no topic specified and no default topic prefix configured for %s.%s", entity.Type, action)
			}
		}
	}
	// --- End Determine Topic ---

	if topic == "" {
		// This check is likely redundant now due to the logic above, but keep for safety
		return fmt.Errorf("could not determine topic for %s.%s", entity.Type, action)
	}

	// Publish message
	return c.producer.Publish(topic, eventMsg)
}

// RegisterHandler registers an event handler for a specific event type
func (c *EventClient) RegisterHandler(entityType, action string, handler func(context.Context, *messages.BaseEventMessage) error) {
	if c.consumer == nil {
		panic("Cannot register handler: consumer not initialized")
	}

	basicHandler := consumer.NewBasicHandler(handler)
	c.consumer.RegisterHandler(entityType, action, basicHandler)
}

// RegisterTypedHandler registers a handler for events with a specific payload type
func RegisterTypedHandler[T any](client *EventClient, entityType, action string, handler func(context.Context, *messages.TypedEventMessage[T]) error) {
	if client.consumer == nil {
		panic("Cannot register handler: consumer not initialized")
	}

	typedHandler := consumer.NewTypedHandler(handler)
	client.consumer.RegisterHandler(entityType, action, typedHandler)
}

// Start starts the event client
func (c *EventClient) Start(ctx context.Context) error {
	if c.consumer == nil {
		// No consumer to start
		return nil
	}

	return c.consumer.Start(ctx)
}

// Close cleans up resources
func (c *EventClient) Close() error {
	var producerErr, consumerErr error

	if c.producer != nil {
		producerErr = c.producer.Close()
	}

	if c.consumer != nil {
		consumerErr = c.consumer.Stop()
	}

	if producerErr != nil {
		return producerErr
	}
	return consumerErr
}
