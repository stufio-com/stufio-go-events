package consumer

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/stufio-com/stufio-go-events/config"
	"github.com/stufio-com/stufio-go-events/messages"
)

// EventHandler defines the interface for handling event messages
type EventHandler interface {
	// HandleEvent processes a received event message
	HandleEvent(ctx context.Context, eventMsg *messages.BaseEventMessage) error

	// HandleEventWithMessage processes a received event with the raw Kafka message
	HandleEventWithMessage(ctx context.Context, eventMsg *messages.BaseEventMessage, message *sarama.ConsumerMessage) error
}

// EventConsumer consumes Kafka event messages
type EventConsumer struct {
	config        *config.KafkaConfig
	client        sarama.ConsumerGroup
	handlers      map[string][]EventHandler // maps topic.entityType.action to handlers
	topicHandlers map[string][]EventHandler // maps topic to handlers
	ready         chan bool
	consuming     bool
	mu            sync.Mutex
	wg            sync.WaitGroup
	errorHandler  func(error)
}

// NewEventConsumer creates a new Kafka event consumer
func NewEventConsumer(config *config.KafkaConfig) (*EventConsumer, error) {
	saramaConfig, err := config.CreateConsumerConfig()
	if err != nil {
		return nil, fmt.Errorf("creating consumer config: %w", err)
	}

	client, err := sarama.NewConsumerGroup(config.GetBrokersList(), config.GroupID, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("creating consumer group: %w", err)
	}

	consumer := &EventConsumer{
		config:        config,
		client:        client,
		handlers:      make(map[string][]EventHandler),
		topicHandlers: make(map[string][]EventHandler),
		ready:         make(chan bool),
		errorHandler:  defaultErrorHandler,
	}

	return consumer, nil
}

// RegisterHandler registers an event handler for a specific event type
func (c *EventConsumer) RegisterHandler(entityType, action string, handler EventHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := fmt.Sprintf("%s.%s", entityType, action)
	c.handlers[key] = append(c.handlers[key], handler)
}

// RegisterTopicHandler registers a handler for all events on a specific topic
func (c *EventConsumer) RegisterTopicHandler(topic string, handler EventHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.topicHandlers[topic] = append(c.topicHandlers[topic], handler)
}

// SetErrorHandler sets a custom error handler function
func (c *EventConsumer) SetErrorHandler(handler func(error)) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.errorHandler = handler
}

// Start begins consuming messages
func (c *EventConsumer) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.consuming {
		c.mu.Unlock()
		return fmt.Errorf("consumer already running")
	}
	c.consuming = true
	c.mu.Unlock()

	// Create a cancellable context
	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Handle errors from consumer group
	go func() {
		for err := range c.client.Errors() {
			c.handleError(err)
		}
	}()

	// Capture signals for graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	c.wg.Add(1)

	// Start consuming in a goroutine
	go func() {
		defer c.wg.Done()

		for {
			// Check if context is cancelled before Consume call
			select {
			case <-consumeCtx.Done():
				log.Println("Context cancelled, stopping consumer")
				return
			default:
			}

			// Consume the topics
			if err := c.client.Consume(consumeCtx, c.config.Topics, c); err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					log.Println("Consumer group closed")
					return
				}
				c.handleError(fmt.Errorf("consume error: %w", err))
			}

			// Check if consumer was marked as ready, if not, we broke out of Consume
			// because the context was cancelled, so exit
			select {
			case <-c.ready:
			case <-consumeCtx.Done():
				return
			}

			// Reset the ready channel for next Consume call
			c.ready = make(chan bool)
		}
	}()

	// Wait for consumer setup
	<-c.ready
	log.Println("Consumer up and running")

	// Wait for termination signal or context cancellation
	select {
	case <-signals:
		log.Println("Received termination signal, stopping consumer")
	case <-ctx.Done():
		log.Println("Context cancelled, stopping consumer")
	}

	// Trigger cancellation of the consume context
	cancel()

	// Wait for consumption to stop
	c.wg.Wait()

	// Close the consumer group
	if err := c.client.Close(); err != nil {
		return fmt.Errorf("closing consumer group: %w", err)
	}

	c.mu.Lock()
	c.consuming = false
	c.mu.Unlock()

	return nil
}

// Stop gracefully shuts down the consumer
func (c *EventConsumer) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.consuming {
		return nil
	}

	if err := c.client.Close(); err != nil {
		return fmt.Errorf("closing consumer group: %w", err)
	}

	c.consuming = false
	return nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *EventConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *EventConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *EventConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Create a context that inherits from the session context
	ctx := session.Context()

	for message := range claim.Messages() {
		// Process message
		err := c.processMessage(ctx, message)
		if err != nil {
			c.handleError(fmt.Errorf("processing message: %w", err))
			// Continue processing other messages despite errors
		}

		// Mark the message as processed
		session.MarkMessage(message, "")

		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}

	return nil
}

// processMessage handles a received message
func (c *EventConsumer) processMessage(ctx context.Context, message *sarama.ConsumerMessage) error {
	// First extract entity_type and action from headers if available
	var entityType, action string
	for _, header := range message.Headers {
		if string(header.Key) == "entity_type" {
			entityType = string(header.Value)
		} else if string(header.Key) == "action" {
			action = string(header.Value)
		}
	}

	// Parse message into BaseEventMessage
	eventMsg, err := messages.FromJSON(message.Value)
	if err != nil {
		return fmt.Errorf("parsing event message: %w", err)
	}

	// If headers are empty, use values from the payload
	if entityType == "" {
		entityType = eventMsg.Entity.Type
	}
	if action == "" {
		action = eventMsg.Action
	}

	// Override payload values with header values (header values take precedence)
	// This ensures consistency between headers and the handler lookup
	eventMsg.Entity.Type = entityType
	eventMsg.Action = action

	// Get handlers for this event type
	key := fmt.Sprintf("%s.%s", entityType, action)

	log.Printf("Processing message: topic=%s, entity_type=%s, action=%s",
		message.Topic, entityType, action)

	c.mu.Lock()
	eventHandlers := make([]EventHandler, len(c.handlers[key]))
	copy(eventHandlers, c.handlers[key])

	// Also get any handlers registered for the topic
	topicHandlers := make([]EventHandler, len(c.topicHandlers[message.Topic]))
	copy(topicHandlers, c.topicHandlers[message.Topic])
	c.mu.Unlock()

	if len(eventHandlers) == 0 && len(topicHandlers) == 0 {
		// No handlers for this message, log and skip
		log.Printf("No handlers found for message: topic=%s, entity_type=%s, action=%s",
			message.Topic, entityType, action)
		return nil
	}

	// Execute event-specific handlers
	for _, handler := range eventHandlers {
		if err := handler.HandleEventWithMessage(ctx, eventMsg, message); err != nil {
			return fmt.Errorf("handler error for %s: %w", key, err)
		}
	}

	// Execute topic handlers
	for _, handler := range topicHandlers {
		if err := handler.HandleEventWithMessage(ctx, eventMsg, message); err != nil {
			return fmt.Errorf("topic handler error for %s: %w", message.Topic, err)
		}
	}

	return nil
}

func (c *EventConsumer) handleError(err error) {
	c.mu.Lock()
	handler := c.errorHandler
	c.mu.Unlock()

	if handler != nil {
		handler(err)
	}
}

func defaultErrorHandler(err error) {
	log.Printf("Consumer error: %v", err)
}
