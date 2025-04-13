package messages

import (
	"encoding/json"
	"time"
)

// ActorType defines the type of actor who performed an action
type ActorType string

const (
	ActorTypeUser    ActorType = "user"
	ActorTypeAdmin   ActorType = "admin"
	ActorTypeSystem  ActorType = "system"
	ActorTypeService ActorType = "service"
)

// Entity represents an entity involved in an event
type Entity struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// Actor represents the actor who performed an action
type Actor struct {
	Type ActorType `json:"type"`
	ID   string    `json:"id"`
}

// EventMetrics holds performance metrics for event processing
type EventMetrics struct {
	ProcessingTimeMs int            `json:"processing_time_ms,omitempty"`
	DBTimeMs         int            `json:"db_time_ms,omitempty"`
	APITimeMs        int            `json:"api_time_ms,omitempty"`
	QueueTimeMs      int            `json:"queue_time_ms,omitempty"`
	CustomMetrics    map[string]any `json:"custom_metrics,omitempty"`
}

// BaseEventMessage is the standard format for all event messages
type BaseEventMessage struct {
	EventID       string        `json:"event_id"`
	CorrelationID *string       `json:"correlation_id,omitempty"`
	Timestamp     time.Time     `json:"timestamp"`
	Entity        Entity        `json:"entity"`
	Action        string        `json:"action"`
	Actor         Actor         `json:"actor"`
	Payload       any           `json:"payload"`
	Metrics       *EventMetrics `json:"metrics,omitempty"`
}

// TypedEventMessage is a generic typed wrapper for BaseEventMessage with a specific payload type
type TypedEventMessage[T any] struct {
	EventID       string        `json:"event_id"`
	CorrelationID *string       `json:"correlation_id,omitempty"`
	Timestamp     time.Time     `json:"timestamp"`
	Entity        Entity        `json:"entity"`
	Action        string        `json:"action"`
	Actor         Actor         `json:"actor"`
	Payload       T             `json:"payload"`
	Metrics       *EventMetrics `json:"metrics,omitempty"`
}

// EventDefinition defines a specific type of event
type EventDefinition struct {
	Name           string `json:"name"`
	EntityType     string `json:"entity_type"`
	Action         string `json:"action"`
	Description    string `json:"description"`
	Topic          string `json:"topic"`
	HighVolume     bool   `json:"high_volume"`
	RequireActor   bool   `json:"require_actor"`
	RequireEntity  bool   `json:"require_entity"`
	RequirePayload bool   `json:"require_payload"`
}

// FromJSON deserializes a JSON message into an event message
func FromJSON(data []byte) (*BaseEventMessage, error) {
	var msg BaseEventMessage
	err := json.Unmarshal(data, &msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

// FromJSONTyped deserializes a JSON message into a typed event message
func FromJSONTyped[T any](data []byte) (*TypedEventMessage[T], error) {
	var msg TypedEventMessage[T]
	err := json.Unmarshal(data, &msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

// ToJSON serializes an event message to JSON
func (m *BaseEventMessage) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

// ToJSON serializes a typed event message to JSON
func (m *TypedEventMessage[T]) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

// ExtractPayload attempts to convert the raw payload to a specific type
func (m *BaseEventMessage) ExtractPayload(output any) error {
	payloadBytes, err := json.Marshal(m.Payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(payloadBytes, output)
}
