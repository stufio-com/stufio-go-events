package messages

import (
	"encoding/json"
	"fmt"
	"sync"
)

// EventRegistry maintains a mapping between event types and their handlers
type EventRegistry struct {
	mutex       sync.RWMutex
	definitions map[string]EventDefinition
	topicMap    map[string][]string // Maps topics to event names
}

// NewEventRegistry creates a new event registry
func NewEventRegistry() *EventRegistry {
	return &EventRegistry{
		definitions: make(map[string]EventDefinition),
		topicMap:    make(map[string][]string),
	}
}

// global registry instance
var globalRegistry = NewEventRegistry()

// RegisterEvent adds an event definition to the registry
func RegisterEvent(def EventDefinition) {
	globalRegistry.RegisterEvent(def)
}

// GetEventByName retrieves an event definition by name
func GetEventByName(name string) (EventDefinition, bool) {
	return globalRegistry.GetEventByName(name)
}

// FindEventsByTopic returns all event definitions for a given topic
func FindEventsByTopic(topic string) []EventDefinition {
	return globalRegistry.FindEventsByTopic(topic)
}

// LoadEventsFromJSON loads event definitions from a JSON string
func LoadEventsFromJSON(data []byte) error {
	return globalRegistry.LoadEventsFromJSON(data)
}

// RegisterEvent adds an event definition to the registry
func (r *EventRegistry) RegisterEvent(def EventDefinition) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	key := fmt.Sprintf("%s.%s", def.EntityType, def.Action)
	r.definitions[key] = def

	// Update topic map
	events, exists := r.topicMap[def.Topic]
	if !exists {
		events = []string{}
	}
	r.topicMap[def.Topic] = append(events, key)
}

// GetEventByName retrieves an event definition by name
func (r *EventRegistry) GetEventByName(name string) (EventDefinition, bool) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	def, ok := r.definitions[name]
	return def, ok
}

// FindEventsByTopic returns all event definitions for a given topic
func (r *EventRegistry) FindEventsByTopic(topic string) []EventDefinition {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	var results []EventDefinition

	eventNames, ok := r.topicMap[topic]
	if !ok {
		return results
	}

	for _, name := range eventNames {
		if def, ok := r.definitions[name]; ok {
			results = append(results, def)
		}
	}

	return results
}

// LoadEventsFromJSON loads event definitions from a JSON string
func (r *EventRegistry) LoadEventsFromJSON(data []byte) error {
	var definitions []EventDefinition
	if err := json.Unmarshal(data, &definitions); err != nil {
		return fmt.Errorf("unmarshalling event definitions: %w", err)
	}

	for _, def := range definitions {
		r.RegisterEvent(def)
	}

	return nil
}
