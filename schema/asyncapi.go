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
	schemaMaps, _, err := l.ExtractPayloadSchemasAndRefs()
	return schemaMaps, err
}

// ExtractPayloadSchemasAndRefs extracts payload schemas and their original $ref strings.
// Returns:
// 1. map[string]map[string]interface{}: "entityType.action" -> resolved raw payload schema map.
// 2. map[string]string: "entityType.action" -> CORRECTED $ref string (e.g., "#/components/schemas/MyPayload").
// 3. error: Any error encountered during extraction.
func (l *SchemaLoader) ExtractPayloadSchemasAndRefs() (map[string]map[string]interface{}, map[string]string, error) {
	if l.schema == nil {
		return nil, nil, fmt.Errorf("no schema loaded")
	}

	schemaMaps := make(map[string]map[string]interface{})
	schemaRefs := make(map[string]string) // Map to store CORRECTED refs

	for channelName, channel := range l.schema.Channels {
		parts := strings.Split(channelName, ".")
		if len(parts) != 4 {
			log.Printf("Skipping channel '%s': Name does not match expected format '_.events.entity.action'", channelName)
			continue
		}
		entityType := parts[2]
		action := parts[3]
		key := fmt.Sprintf("%s.%s", entityType, action)

		var messageDef *Message
		var operationDesc string // To potentially get description

		// Check publish and subscribe operations for the message
		if channel.Publish != nil && channel.Publish.Message != nil {
			messageDef = channel.Publish.Message
			operationDesc = channel.Publish.Description
		} else if channel.Subscribe != nil && channel.Subscribe.Message != nil {
			messageDef = channel.Subscribe.Message
			operationDesc = channel.Subscribe.Description
		} else {
			log.Printf("Warning: No message definition found for channel %s", channelName)
			continue
		}

		// Handle message reference (e.g., $ref: '#/components/messages/UserCreated')
		if msgRef, ok := messageDef.Payload["$ref"].(string); ok {
			if !strings.HasPrefix(msgRef, "#/components/messages/") {
				log.Printf("Warning: Unsupported message reference format '%s' in channel %s", msgRef, channelName)
				continue
			}
			msgName := strings.TrimPrefix(msgRef, "#/components/messages/")
			if compMsg, found := l.schema.Components.Messages[msgName]; found {
				messageDef = &compMsg // Point to the component message
			} else {
				log.Printf("Warning: Message reference '%s' not found in components for channel %s", msgRef, channelName)
				continue
			}
		}

		// Now get the payload definition from the resolved messageDef
		if messageDef.Payload == nil {
			log.Printf("Warning: No payload definition found for message in channel %s", channelName)
			continue // Skip if no payload defined
		}

		// --- Get the ORIGINAL payload $ref ---
		originalPayloadRef, ok := messageDef.Payload["$ref"].(string)
		if !ok {
			log.Printf("Warning: Payload for %s is not a $ref, cannot process.", key)
			continue
		}

		// --- PARSE the original ref to get the ACTUAL payload schema name ---
		actualSchemaName, err := parseActualSchemaName(originalPayloadRef)
		if err != nil {
			log.Printf("Warning: Could not parse actual schema name from ref '%s' for %s: %v", originalPayloadRef, key, err)
			continue
		}

		// --- Construct the CORRECTED reference string ---
		correctedRef := fmt.Sprintf("#/components/schemas/%s", actualSchemaName)
		schemaRefs[key] = correctedRef // Store the CORRECTED ref

		// --- Resolve the ref using the CORRECTED ref string ---
		// Create a temporary map with the corrected ref to pass to resolveSchemaRef
		tempSchemaDef := map[string]interface{}{"$ref": correctedRef}
		resolvedPayloadSchema, err := l.resolveSchemaRef(tempSchemaDef) // Use existing resolve function with corrected ref
		if err != nil {
			log.Printf("Warning: Could not resolve corrected payload schema ref '%s' for %s: %v", correctedRef, key, err)
			continue
		}

		// Add description if missing in the resolved schema, using message or operation description
		if _, descExists := resolvedPayloadSchema["description"]; !descExists {
			desc := messageDef.Description
			if desc == "" {
				desc = operationDesc
			}
			if desc != "" {
				// Use the actual schema name for the description title
				resolvedPayloadSchema["description"] = fmt.Sprintf("%s: %s", pascalCase(actualSchemaName), desc)
			}
		}

		schemaMaps[key] = resolvedPayloadSchema // Store the resolved map
	}

	return schemaMaps, schemaRefs, nil // Return maps (schemaMaps contains resolved, schemaRefs contains corrected refs)
}

// --- Add a helper function to parse the actual schema name ---
// Parses "BaseEventMessage[ActualName]" or "ActualName" from a ref like "#/components/schemas/..."
func parseActualSchemaName(ref string) (string, error) {
	if !strings.HasPrefix(ref, "#/components/schemas/") {
		return "", fmt.Errorf("ref does not start with #/components/schemas/")
	}
	namePart := strings.TrimPrefix(ref, "#/components/schemas/")

	// Check for BaseEventMessage[ActualName] pattern
	if strings.HasPrefix(namePart, "BaseEventMessage[") && strings.HasSuffix(namePart, "]") {
		innerName := strings.TrimPrefix(namePart, "BaseEventMessage[")
		innerName = strings.TrimSuffix(innerName, "]")
		if innerName == "" {
			return "", fmt.Errorf("invalid BaseEventMessage pattern, empty inner name in ref: %s", ref)
		}
		return innerName, nil
	}
	// Otherwise, assume the name part is the actual schema name
	if namePart == "" {
		return "", fmt.Errorf("empty schema name in ref: %s", ref)
	}
	return namePart, nil
}

// Update resolveSchemaRef to handle only direct schema names from components
func (l *SchemaLoader) resolveSchemaRef(schemaDef map[string]interface{}) (map[string]interface{}, error) {
	if schemaDef == nil {
		return nil, fmt.Errorf("schema definition is nil")
	}

	ref, ok := schemaDef["$ref"].(string)
	if !ok {
		// If not a ref, assume it's an inline schema. Return it as is.
		// The caller (ExtractPayloadSchemasAndRefs) should handle adding descriptions etc.
		// The generator needs to handle inline object definitions separately if needed.
		log.Printf("DEBUG: resolveSchemaRef found inline schema (not a $ref): %v", schemaDef)
		return schemaDef, nil
	}

	// Use the new parser function here as well for consistency, although
	// ExtractPayloadSchemasAndRefs should already pass the corrected ref.
	actualSchemaName, err := parseActualSchemaName(ref)
	if err != nil {
		return nil, fmt.Errorf("resolving ref '%s': %w", ref, err)
	}

	// Look up in components
	if l.schema == nil || l.schema.Components.Schemas == nil {
		return nil, fmt.Errorf("schema or components not loaded")
	}

	componentSchema, found := l.schema.Components.Schemas[actualSchemaName]
	if !found {
		return nil, fmt.Errorf("schema '%s' not found in components (from ref '%s')", actualSchemaName, ref)
	}

	componentSchemaMap, ok := componentSchema.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("component schema '%s' is not a valid map[string]interface{}", actualSchemaName)
	}

	// Return the raw map.
	return componentSchemaMap, nil
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
