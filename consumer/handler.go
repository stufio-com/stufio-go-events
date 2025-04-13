package consumer

import (
	"context"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/stufio-com/stufio-go-events/messages"
)

// BasicEventHandler provides a simple implementation of EventHandler
type BasicEventHandler struct {
	handlerFunc func(context.Context, *messages.BaseEventMessage) error
}

// NewBasicHandler creates a new basic event handler
func NewBasicHandler(handler func(context.Context, *messages.BaseEventMessage) error) *BasicEventHandler {
	return &BasicEventHandler{
		handlerFunc: handler,
	}
}

// HandleEvent implements the EventHandler interface
func (h *BasicEventHandler) HandleEvent(ctx context.Context, eventMsg *messages.BaseEventMessage) error {
	return h.handlerFunc(ctx, eventMsg)
}

// HandleEventWithMessage implements the EventHandler interface with access to raw message
func (h *BasicEventHandler) HandleEventWithMessage(ctx context.Context, eventMsg *messages.BaseEventMessage, message *sarama.ConsumerMessage) error {
	return h.HandleEvent(ctx, eventMsg)
}

// TypedEventHandler handles events with a specific payload type
type TypedEventHandler[T any] struct {
	handlerFunc func(context.Context, *messages.TypedEventMessage[T]) error
}

// NewTypedHandler creates a new typed event handler
func NewTypedHandler[T any](handler func(context.Context, *messages.TypedEventMessage[T]) error) *TypedEventHandler[T] {
	return &TypedEventHandler[T]{
		handlerFunc: handler,
	}
}

// HandleEvent implements the EventHandler interface
func (h *TypedEventHandler[T]) HandleEvent(ctx context.Context, eventMsg *messages.BaseEventMessage) error {
	// Convert to typed event message
	var payload T
	if err := eventMsg.ExtractPayload(&payload); err != nil {
		return fmt.Errorf("extracting typed payload: %w", err)
	}

	// Create typed event message
	typedMsg := &messages.TypedEventMessage[T]{
		EventID:       eventMsg.EventID,
		CorrelationID: eventMsg.CorrelationID,
		Timestamp:     eventMsg.Timestamp,
		Entity:        eventMsg.Entity,
		Action:        eventMsg.Action,
		Actor:         eventMsg.Actor,
		Payload:       payload,
		Metrics:       eventMsg.Metrics,
	}

	return h.handlerFunc(ctx, typedMsg)
}

// HandleEventWithMessage implements the EventHandler interface with access to raw message
func (h *TypedEventHandler[T]) HandleEventWithMessage(ctx context.Context, eventMsg *messages.BaseEventMessage, message *sarama.ConsumerMessage) error {
	return h.HandleEvent(ctx, eventMsg)
}

// LoggingHandler logs all events
type LoggingHandler struct {
	LogLevel string
}

// NewLoggingHandler creates a new logging handler
func NewLoggingHandler(logLevel string) *LoggingHandler {
	return &LoggingHandler{
		LogLevel: logLevel,
	}
}

// HandleEvent implements the EventHandler interface
func (h *LoggingHandler) HandleEvent(ctx context.Context, eventMsg *messages.BaseEventMessage) error {
	log.Printf("[%s] Event: %s.%s, ID: %s, Entity: %s/%s, Actor: %s/%s",
		h.LogLevel,
		eventMsg.Entity.Type,
		eventMsg.Action,
		eventMsg.EventID,
		eventMsg.Entity.Type,
		eventMsg.Entity.ID,
		eventMsg.Actor.Type,
		eventMsg.Actor.ID,
	)
	return nil
}

// HandleEventWithMessage implements the EventHandler interface
func (h *LoggingHandler) HandleEventWithMessage(ctx context.Context, eventMsg *messages.BaseEventMessage, message *sarama.ConsumerMessage) error {
	log.Printf("[%s] Event: %s.%s, ID: %s, Topic: %s, Partition: %d, Offset: %d",
		h.LogLevel,
		eventMsg.Entity.Type,
		eventMsg.Action,
		eventMsg.EventID,
		message.Topic,
		message.Partition,
		message.Offset,
	)
	return nil
}

// EventMultiplexer distributes events to multiple handlers
type EventMultiplexer struct {
	handlers []EventHandler
}

// NewEventMultiplexer creates a new event multiplexer
func NewEventMultiplexer(handlers ...EventHandler) *EventMultiplexer {
	return &EventMultiplexer{
		handlers: handlers,
	}
}

// AddHandler adds a handler to the multiplexer
func (m *EventMultiplexer) AddHandler(handler EventHandler) {
	m.handlers = append(m.handlers, handler)
}

// HandleEvent implements the EventHandler interface
func (m *EventMultiplexer) HandleEvent(ctx context.Context, eventMsg *messages.BaseEventMessage) error {
	for _, handler := range m.handlers {
		if err := handler.HandleEvent(ctx, eventMsg); err != nil {
			return err
		}
	}
	return nil
}

// HandleEventWithMessage implements the EventHandler interface with access to raw message
func (m *EventMultiplexer) HandleEventWithMessage(ctx context.Context, eventMsg *messages.BaseEventMessage, message *sarama.ConsumerMessage) error {
	for _, handler := range m.handlers {
		if err := handler.HandleEventWithMessage(ctx, eventMsg, message); err != nil {
			return err
		}
	}
	return nil
}
