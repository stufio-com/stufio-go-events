package schema

import (
	"encoding/json"
	"fmt"
	"io"
	"log" // Import log package
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/stufio-com/stufio-go-events/messages"
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
// Returns a map of "entityType.action" to the *resolved* raw payload schema map.
func (l *SchemaLoader) ExtractPayloadSchemas() (map[string]map[string]interface{}, error) {
	if l.schema == nil {
		return nil, fmt.Errorf("no schema loaded")
	}

	// Change the map type to store raw schema maps instead of compiled schemas
	schemas := make(map[string]map[string]interface{})

	// Process each channel
	for channelName, channel := range l.schema.Channels {
		// Try to extract entity type and action from channel name (expecting nameniac.events.entity.action)
		parts := strings.Split(channelName, ".")
		if len(parts) != 4 { // Expecting 4 parts
			log.Printf("Warning: Skipping channel with unexpected name format: %s", channelName)
			continue // Skip channels without proper naming
		}

		entityType := parts[2]                          // Use 3rd part
		action := parts[3]                              // Use 4th part
		key := fmt.Sprintf("%s.%s", entityType, action) // e.g., "user.created"

		// Get the message definition ($ref or inline)
		var messageDef *Message
		if channel.Publish != nil && channel.Publish.Message != nil {
			messageDef = channel.Publish.Message
		} else if channel.Subscribe != nil && channel.Subscribe.Message != nil {
			messageDef = channel.Subscribe.Message
		} else {
			continue // No message definition
		}

		// Get the payload definition from the message
		payloadDefinition := messageDef.Payload
		if payloadDefinition == nil {
			log.Printf("Warning: No payload definition found for message in channel %s", channelName)
			continue
		}

		// Extract the *actual* payload schema reference (e.g., #/components/schemas/UserCreatedPayload)
		actualPayloadSchemaRef, err := l.extractActualPayloadSchemaRef(payloadDefinition)
		if err != nil {
			log.Printf("Warning: Could not extract actual payload schema ref for %s (%s): %v", key, channelName, err)
			continue
		}

		// Resolve this reference to the actual schema definition map
		resolvedPayloadSchema, err := l.resolveSchemaRef(map[string]interface{}{"$ref": actualPayloadSchemaRef})
		if err != nil {
			log.Printf("Warning: Could not resolve actual payload schema %s for %s (%s): %v", actualPayloadSchemaRef, key, channelName, err)
			continue
		}

		// Store the resolved schema map (compilation will happen later if needed, e.g., for validation)
		schemas[key] = resolvedPayloadSchema

		// Optional: Compile here for immediate validation feedback during generation (uncomment if needed)
		/*
			loader := gojsonschema.NewGoLoader(resolvedPayloadSchema)
			_, err = gojsonschema.NewSchema(loader)
			if err != nil {
				rawJSON, _ := json.MarshalIndent(resolvedPayloadSchema, "", "  ")
				log.Printf("Error compiling schema for %s: %v\nSchema:\n%s", key, err, string(rawJSON))
				continue // Skip schema if compilation fails
			}
		*/
	}

	return schemas, nil
}

// extractPayloadSchema processes a payload definition to make it a valid JSON Schema
// Only adds defaults if it's NOT a reference schema.
func extractPayloadSchema(payload map[string]interface{}) (map[string]interface{}, error) {
	if payload == nil {
		return nil, fmt.Errorf("missing payload schema")
	}

	// If it's a reference, return as is.
	if _, isRef := payload["$ref"].(string); isRef {
		return payload, nil
	}

	// Ensure schema has required JSON Schema fields for inline schemas
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

// Add this new helper method to SchemaLoader
// extractActualPayloadSchemaRef extracts the direct payload schema reference
// (e.g., #/components/schemas/UserCreatedPayload) from a message payload definition
// that might use patterns like BaseEventMessage[UserCreatedPayload].
func (l *SchemaLoader) extractActualPayloadSchemaRef(payloadDef map[string]interface{}) (string, error) {
	if payloadDef == nil {
		return "", fmt.Errorf("payload definition is nil")
	}

	var refToCheck string

	// Check for direct $ref
	if ref, ok := payloadDef["$ref"].(string); ok {
		refToCheck = ref
	} else if oneOf, ok := payloadDef["oneOf"].([]interface{}); ok && len(oneOf) > 0 {
		// Check inside oneOf
		if refMap, ok := oneOf[0].(map[string]interface{}); ok {
			if ref, ok := refMap["$ref"].(string); ok {
				refToCheck = ref
			}
		}
	} else if allOf, ok := payloadDef["allOf"].([]interface{}); ok && len(allOf) > 0 {
		// Check inside allOf (less common for the top-level payload def, but possible)
		if refMap, ok := allOf[0].(map[string]interface{}); ok {
			if ref, ok := refMap["$ref"].(string); ok {
				refToCheck = ref
			}
		}
	}

	if refToCheck == "" {
		return "", fmt.Errorf("could not find $ref in payload definition structure: %v", payloadDef)
	}

	// Check if it's a local components ref
	if !strings.HasPrefix(refToCheck, "#/components/schemas/") {
		return "", fmt.Errorf("payload $ref does not point to #/components/schemas/: %s", refToCheck)
	}

	// Check if it's the BaseEventMessage[Param] pattern and extract Param
	if strings.Contains(refToCheck, "[") && strings.HasSuffix(refToCheck, "]") && strings.Contains(refToCheck, "BaseEventMessage") {
		parts := strings.SplitN(refToCheck, "[", 2)
		paramName := strings.TrimSuffix(parts[1], "]")
		// Return the reference to the parameter schema
		return fmt.Sprintf("#/components/schemas/%s", paramName), nil
	}

	// Otherwise, assume the ref points directly to the payload schema
	return refToCheck, nil
}

// Update resolveSchemaRef to handle only direct schema names from components
func (l *SchemaLoader) resolveSchemaRef(schemaDef map[string]interface{}) (map[string]interface{}, error) {
	if schemaDef == nil {
		return nil, fmt.Errorf("schema definition is nil")
	}

	ref, ok := schemaDef["$ref"].(string)
	if !ok {
		// If not a ref, assume it's an inline schema. Add basic validation/defaults.
		return extractPayloadSchema(schemaDef)
	}

	// Basic $ref parsing (expecting #/components/schemas/ActualSchemaName)
	if !strings.HasPrefix(ref, "#/components/schemas/") {
		return nil, fmt.Errorf("unsupported or unexpected $ref format: %s", ref)
	}

	schemaName := strings.TrimPrefix(ref, "#/components/schemas/")

	// Ensure no BaseEventMessage[...] pattern remains here
	if strings.Contains(schemaName, "[") {
		return nil, fmt.Errorf("resolveSchemaRef received unexpected parameterized ref: %s", schemaName)
	}

	// Look up in components
	if l.schema == nil || l.schema.Components.Schemas == nil {
		return nil, fmt.Errorf("schema or components section not loaded/present")
	}

	componentSchema, found := l.schema.Components.Schemas[schemaName]
	if !found {
		return nil, fmt.Errorf("referenced schema not found in components: %s", schemaName)
	}

	componentSchemaMap, ok := componentSchema.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("component schema '%s' is not a valid object", schemaName)
	}

	// Return the raw map. The generator will process this.
	// We might need to recursively resolve internal $refs within this map later if the generator needs it.
	return componentSchemaMap, nil
}

// GetSchemaComponents returns the Components part of the loaded schema.
func (l *SchemaLoader) GetSchemaComponents() (Components, bool) {
	if l.schema == nil {
		return Components{}, false
	}
	return l.schema.Components, true
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
	// Try to extract entity type and action from channel name (expecting nameniac.events.entity.action)
	parts := strings.Split(channelName, ".")
	entityType := "unknown"
	action := "unknown"

	if len(parts) == 4 { // Updated check for 4 parts
		entityType = parts[2] // Use 3rd part
		action = parts[3]     // Use 4th part
	} else {
		log.Printf("Warning: Could not parse entity/action from channel name: %s", channelName)
	}

	// Use description from operation if available
	if operation.Description != "" {
		description = operation.Description
	}

	// Use message title as name if available, otherwise construct from entity/action
	name := fmt.Sprintf("%s.%s", entityType, action) // Default name
	if operation.Message != nil && operation.Message.Title != "" {
		// Prefer message title if it exists and is meaningful
		// Example title: "nameniac.events.domain.discovered:Message" - clean it
		cleanedTitle := strings.Split(operation.Message.Title, ":")[0]
		// Further clean based on expected pattern "nameniac.events.entity.action"
		titleParts := strings.Split(cleanedTitle, ".")
		if len(titleParts) == 4 {
			name = fmt.Sprintf("%s.%s", titleParts[2], titleParts[3])
		} else if cleanedTitle != "" {
			name = cleanedTitle // Fallback to cleaned title if pattern doesn't match
		}
	}

	// Determine if high volume based on name patterns
	highVolume := strings.Contains(strings.ToLower(name), "high_volume") ||
		strings.Contains(strings.ToLower(name), "notification") ||
		strings.Contains(strings.ToLower(description), "high volume") ||
		// Add specific known high-volume events if needed
		name == "user.activity" || name == "domain.searched"

	return messages.EventDefinition{
		Name:           name, // Use the potentially cleaned title or entity.action
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
