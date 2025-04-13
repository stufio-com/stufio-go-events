package schema

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/stufio-com/stufio-go-events/messages"
	"github.com/xeipuuv/gojsonschema"
)

// AsyncAPISchema represents an AsyncAPI schema document
type AsyncAPISchema struct {
	AsyncAPI           string                 `json:"asyncapi"`
	Info               map[string]interface{} `json:"info"`
	Channels           map[string]Channel     `json:"channels"`
	Components         Components             `json:"components,omitempty"`
	Servers            map[string]interface{} `json:"servers,omitempty"`
	DefaultContentType string                 `json:"defaultContentType,omitempty"`
}

// Channel represents an AsyncAPI channel
type Channel struct {
	Description string                 `json:"description,omitempty"`
	Subscribe   *Operation             `json:"subscribe,omitempty"`
	Publish     *Operation             `json:"publish,omitempty"`
	Parameters  map[string]interface{} `json:"parameters,omitempty"`
	Bindings    map[string]interface{} `json:"bindings,omitempty"`
}

// Operation represents an AsyncAPI operation
type Operation struct {
	Summary     string                 `json:"summary,omitempty"`
	Description string                 `json:"description,omitempty"`
	Message     *Message               `json:"message,omitempty"`
	Bindings    map[string]interface{} `json:"bindings,omitempty"`
}

// Message represents an AsyncAPI message
type Message struct {
	Name        string                 `json:"name,omitempty"`
	Title       string                 `json:"title,omitempty"`
	Summary     string                 `json:"summary,omitempty"`
	Description string                 `json:"description,omitempty"`
	Payload     map[string]interface{} `json:"payload,omitempty"`
	Headers     map[string]interface{} `json:"headers,omitempty"`
}

// Components represents AsyncAPI components
type Components struct {
	Schemas    map[string]interface{} `json:"schemas,omitempty"`
	Messages   map[string]Message     `json:"messages,omitempty"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
	Examples   map[string]interface{} `json:"examples,omitempty"`
}

// SchemaLoader handles loading of AsyncAPI schemas
type SchemaLoader struct {
	schema *AsyncAPISchema
}

// NewSchemaLoader creates a new schema loader
func NewSchemaLoader() *SchemaLoader {
	return &SchemaLoader{}
}

// LoadFromFile loads an AsyncAPI schema from a file
func (l *SchemaLoader) LoadFromFile(filePath string) error {
	// Check if file exists
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return fmt.Errorf("schema file not found: %s", filePath)
	}

	// Read file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("reading schema file: %w", err)
	}

	// Parse schema based on file extension
	ext := filepath.Ext(filePath)
	switch strings.ToLower(ext) {
	case ".json":
		return l.LoadFromJSON(data)
	case ".yaml", ".yml":
		return fmt.Errorf("YAML schemas not yet supported")
	default:
		return fmt.Errorf("unsupported schema file format: %s", ext)
	}
}

// LoadFromURL loads an AsyncAPI schema from a URL
func (l *SchemaLoader) LoadFromURL(url string) error {
	// Fetch the schema
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("fetching schema from URL: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP error fetching schema: %s", resp.Status)
	}

	// Read response body
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading schema response: %w", err)
	}

	// Determine format based on Content-Type header
	contentType := resp.Header.Get("Content-Type")
	if strings.Contains(contentType, "json") {
		return l.LoadFromJSON(data)
	} else if strings.Contains(contentType, "yaml") || strings.Contains(contentType, "yml") {
		return fmt.Errorf("YAML schemas not yet supported")
	}

	// Try to determine format from URL
	if strings.HasSuffix(url, ".json") {
		return l.LoadFromJSON(data)
	} else if strings.HasSuffix(url, ".yaml") || strings.HasSuffix(url, ".yml") {
		return fmt.Errorf("YAML schemas not yet supported")
	}

	// Default to JSON
	return l.LoadFromJSON(data)
}

// LoadFromJSON loads an AsyncAPI schema from JSON data
func (l *SchemaLoader) LoadFromJSON(data []byte) error {
	var schema AsyncAPISchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("parsing JSON schema: %w", err)
	}

	// Validate basic structure
	if schema.AsyncAPI == "" {
		return fmt.Errorf("invalid AsyncAPI schema: missing asyncapi version")
	}

	l.schema = &schema
	return nil
}

// ExtractEventDefinitions extracts event definitions from the schema
func (l *SchemaLoader) ExtractEventDefinitions() ([]messages.EventDefinition, error) {
	if l.schema == nil {
		return nil, fmt.Errorf("no schema loaded")
	}

	var definitions []messages.EventDefinition

	// Process each channel
	for channelName, channel := range l.schema.Channels {
		// Find Kafka topic from bindings
		topic := extractKafkaTopic(channel)
		if topic == "" {
			topic = channelName // Default to channel name if no binding
		}

		// Process publish operation (events we can consume)
		if channel.Publish != nil && channel.Publish.Message != nil {
			event, err := extractEventFromOperation(channelName, topic, channel.Publish, channel.Description)
			if err != nil {
				return nil, fmt.Errorf("extracting publish event from channel %s: %w", channelName, err)
			}
			definitions = append(definitions, event)
		}

		// Process subscribe operation (events we can produce)
		if channel.Subscribe != nil && channel.Subscribe.Message != nil {
			event, err := extractEventFromOperation(channelName, topic, channel.Subscribe, channel.Description)
			if err != nil {
				return nil, fmt.Errorf("extracting subscribe event from channel %s: %w", channelName, err)
			}
			definitions = append(definitions, event)
		}
	}

	return definitions, nil
}

// ExtractPayloadSchemas extracts payload schemas from the schema
func (l *SchemaLoader) ExtractPayloadSchemas() (map[string]*gojsonschema.Schema, error) {
	if l.schema == nil {
		return nil, fmt.Errorf("no schema loaded")
	}

	schemas := make(map[string]*gojsonschema.Schema)

	// Process each channel
	for channelName, channel := range l.schema.Channels {
		// Try to extract entity type and action from channel name
		parts := strings.Split(channelName, ".")
		if len(parts) < 2 {
			continue // Skip channels without proper naming
		}

		entityType := parts[0]
		action := parts[1]
		key := fmt.Sprintf("%s.%s", entityType, action)

		// Extract payload schema from publish operation
		if channel.Publish != nil && channel.Publish.Message != nil {
			payloadSchema, err := extractPayloadSchema(channel.Publish.Message.Payload)
			if err != nil {
				return nil, fmt.Errorf("extracting payload schema for %s: %w", key, err)
			}

			// Compile schema for validation
			loader := gojsonschema.NewGoLoader(payloadSchema)
			schema, err := gojsonschema.NewSchema(loader)
			if err != nil {
				return nil, fmt.Errorf("compiling schema for %s: %w", key, err)
			}

			schemas[key] = schema
		}

		// Extract payload schema from subscribe operation
		if channel.Subscribe != nil && channel.Subscribe.Message != nil {
			payloadSchema, err := extractPayloadSchema(channel.Subscribe.Message.Payload)
			if err != nil {
				return nil, fmt.Errorf("extracting payload schema for %s: %w", key, err)
			}

			// Compile schema for validation
			loader := gojsonschema.NewGoLoader(payloadSchema)
			schema, err := gojsonschema.NewSchema(loader)
			if err != nil {
				return nil, fmt.Errorf("compiling schema for %s: %w", key, err)
			}

			schemas[key] = schema
		}
	}

	return schemas, nil
}

// extractPayloadSchema processes a payload definition to make it a valid JSON Schema
func extractPayloadSchema(payload map[string]interface{}) (map[string]interface{}, error) {
	if payload == nil {
		return nil, fmt.Errorf("missing payload schema")
	}

	// Ensure schema has required JSON Schema fields
	schema := make(map[string]interface{})
	for k, v := range payload {
		schema[k] = v
	}

	// Add $schema if missing
	if _, exists := schema["$schema"]; !exists {
		schema["$schema"] = "http://json-schema.org/draft-07/schema#"
	}

	// Add type if missing
	if _, exists := schema["type"]; !exists {
		schema["type"] = "object"
	}

	return schema, nil
}

// Helper functions to extract information from the schema

func extractKafkaTopic(channel Channel) string {
	if channel.Bindings == nil {
		return ""
	}

	kafkaBinding, ok := channel.Bindings["kafka"]
	if !ok {
		return ""
	}

	bindingMap, ok := kafkaBinding.(map[string]interface{})
	if !ok {
		return ""
	}

	topic, ok := bindingMap["topic"].(string)
	if !ok {
		return ""
	}

	return topic
}

func extractEventFromOperation(channelName, topic string, operation *Operation, description string) (messages.EventDefinition, error) {
	// Try to extract entity type and action from channel name
	parts := strings.Split(channelName, ".")
	entityType := "unknown"
	action := "unknown"

	if len(parts) >= 2 {
		entityType = parts[0]
		action = parts[1]
	}

	// Use description from operation if available
	if operation.Description != "" {
		description = operation.Description
	}

	// Use message title as name if available
	name := channelName
	if operation.Message != nil && operation.Message.Title != "" {
		name = operation.Message.Title
	}

	// Determine if high volume based on name patterns
	highVolume := strings.Contains(strings.ToLower(name), "high_volume") ||
		strings.Contains(strings.ToLower(name), "notification") ||
		strings.Contains(strings.ToLower(description), "high volume")

	return messages.EventDefinition{
		Name:           name,
		EntityType:     entityType,
		Action:         action,
		Description:    description,
		Topic:          topic,
		HighVolume:     highVolume,
		RequireActor:   true, // Default to requiring actor
		RequireEntity:  true, // Default to requiring entity
		RequirePayload: true, // Default to requiring payload
	}, nil
}
