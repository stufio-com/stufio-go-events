package goevents

import (
	"context"
	"fmt"
	"log"
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
		Timestamp:     time.Now().UTC(),
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

	// --- Determine Topic ---
	topic := topicName
	if topic == "" {
		// Try getting topic from schema registry first
		if eventDef, found := c.schemaRegistry.GetEventDefinition(entity.Type, action); found && eventDef.Topic != "" {
			topic = eventDef.Topic
			log.Printf("Using topic '%s' from schema registry for %s.%s", topic, entity.Type, action)
		} else {
			// Fallback to default topic
			topic = c.config.KafkaTopicPrefix
			log.Printf("Using default topic prefix '%s' for %s.%s (not found in schema)", topic, entity.Type, action)
			if topic == "" {
				return fmt.Errorf("no topic specified and no default topic prefix configured for %s.%s", entity.Type, action)
			}
		}
	}

	if topic == "" {
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

	// Ensure we have topics from schema if none specified
	if len(c.config.Kafka.Topics) == 0 {
		log.Println("No topics configured, fetching from schema registry")
		c.ConfigureConsumerTopics([]string{})
	}

	log.Printf("Starting consumer with topics: %v", c.config.Kafka.Topics)
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

// GetTopicForEvent returns the Kafka topic for a specific event type
func (c *EventClient) GetTopicForEvent(entityType, action string) string {
	topic, found := c.schemaRegistry.GetTopicForEvent(entityType, action)
	if !found {
		// Fallback to default topic prefix if defined
		if c.config.KafkaTopicPrefix != "" {
			return c.config.KafkaTopicPrefix
		}
		// Last resort fallback
		return ""
	}
	return topic
}

// ConfigureConsumerTopics updates the Kafka config with topics from the schema registry
// Keep the original method for backward compatibility
func (c *EventClient) ConfigureConsumerTopics(entityTypes []string) {
	// Convert entity types to entity-action wildcards for backward compatibility
	entityActions := make([]string, len(entityTypes))
	for i, entityType := range entityTypes {
		entityActions[i] = entityType + ".*" // Wildcard for all actions
	}

	c.ConfigureConsumerTopicsForActions(entityActions)
}

// GetConsumerTopics returns the currently configured Kafka topics
func (c *EventClient) GetConsumerTopics() []string {
	return c.config.Kafka.Topics
}

// ConfigureConsumerTopicsForActions provides precise control over entity-action pairs
func (c *EventClient) ConfigureConsumerTopicsForActions(entityActions []string) {
	// Get topics for the specified entity-action pairs
	var topics []string
	if len(entityActions) > 0 {
		topics = c.schemaRegistry.GetTopicsForEntityActions(entityActions)
	} else {
		topics = c.schemaRegistry.GetAllTopics()
	}

	// Ensure we have at least default topic if nothing found
	if len(topics) == 0 && c.config.KafkaTopicPrefix != "" {
		topics = []string{c.config.KafkaTopicPrefix}
	}

	// Update the Kafka config
	c.config.Kafka.Topics = topics

	log.Printf("Configured consumer to use topics: %v for entity-actions: %v", topics, entityActions)
}

// EnsureConsumerInitialized makes sure the consumer is initialized with the configured topics
func (c *EventClient) EnsureConsumerInitialized() error {
	// If consumer is already initialized, nothing to do
	if c.consumer != nil {
		return nil
	}

	// If no topics configured, can't initialize
	if len(c.config.Kafka.Topics) == 0 {
		return fmt.Errorf("cannot initialize consumer: no topics configured")
	}

	// Create the consumer
	var err error
	c.consumer, err = consumer.NewEventConsumer(&c.config.Kafka)
	if err != nil {
		return fmt.Errorf("initializing consumer: %w", err)
	}

	log.Printf("Consumer initialized with topics: %v", c.config.Kafka.Topics)
	return nil
}
