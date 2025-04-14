package schema

import (
	"encoding/json"
	"fmt"
	"log" // Import log
	"os"
	"path/filepath"
	"strings"
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
	Message     *Message               `json:"-"`                 // Ignore direct unmarshalling for Message
	RawMessage  json.RawMessage        `json:"message,omitempty"` // Unmarshal into RawMessage first
	Bindings    map[string]interface{} `json:"bindings,omitempty"`
	Extensions  map[string]interface{} `json:"-"` // Field for extensions (captured in UnmarshalJSON)
}

// UnmarshalJSON implements custom unmarshalling for Operation
// to handle message being inline or a reference AND capture extensions.
func (o *Operation) UnmarshalJSON(data []byte) error {
	// Use a temporary type to avoid recursion with UnmarshalJSON itself
	type Alias Operation
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(o),
	}

	// First, unmarshal known fields (excluding Message and Extensions) into the alias
	if err := json.Unmarshal(data, &aux); err != nil {
		return fmt.Errorf("unmarshalling operation base: %w", err)
	}

	// Now, unmarshal the RawMessage into the Message struct
	// This handles both inline messages and {"$ref": "..."} objects correctly
	// because Message struct also has the Ref field.
	if len(o.RawMessage) > 0 && string(o.RawMessage) != "null" {
		var msg Message
		if err := json.Unmarshal(o.RawMessage, &msg); err != nil {
			// Try to log the problematic raw message for debugging
			log.Printf("DEBUG: Failed to unmarshal operation message: %s", string(o.RawMessage))
			return fmt.Errorf("unmarshalling operation message: %w", err)
		}
		o.Message = &msg
	}

	// --- Capture Extensions (fields starting with 'x-') ---
	var rawMap map[string]interface{}
	if err := json.Unmarshal(data, &rawMap); err != nil {
		// Log or handle error if full unmarshal fails, but might still proceed
		log.Printf("Warning: Could not unmarshal operation fully to capture extensions: %v", err)
	} else {
		o.Extensions = make(map[string]interface{})
		for key, value := range rawMap {
			if strings.HasPrefix(key, "x-") {
				o.Extensions[key] = value
			}
		}
	}
	// --- End Capture Extensions ---

	return nil
}

// Message represents an AsyncAPI message OR a reference to one
type Message struct {
	Ref         string                 `json:"$ref,omitempty"` // Added field to capture top-level $ref
	Name        string                 `json:"name,omitempty"`
	Title       string                 `json:"title,omitempty"`
	Summary     string                 `json:"summary,omitempty"`
	Description string                 `json:"description,omitempty"`
	Payload     map[string]interface{} `json:"payload,omitempty"` // <--- This should capture the payload object
	Headers     map[string]interface{} `json:"headers,omitempty"`
}

// Components represents AsyncAPI components
type Components struct {
	Schemas    map[string]interface{} `json:"schemas,omitempty"`
	Messages   map[string]Message     `json:"messages,omitempty"` // <--- Uses the Message struct
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
		// Basic YAML support could be added using a library like "gopkg.in/yaml.v3"
		// to unmarshal YAML into map[string]interface{} first, then marshal to JSON,
		// and finally unmarshal the JSON into the struct.
		return fmt.Errorf("YAML schemas not yet supported")
	default:
		return fmt.Errorf("unsupported schema file format: %s", ext)
	}
}

// LoadFromURL loads an AsyncAPI schema from a URL
func (l *SchemaLoader) LoadFromURL(url string) error {
	// Fetch the schema using the helper function
	data, err := l.fetchSchemaData(url) // Use fetchSchemaData from loader.go
	if err != nil {
		return err // Error already formatted by fetchSchemaData
	}

	// Determine format based on URL or default to JSON
	// (Assuming fetchSchemaData doesn't handle content-type detection)
	if strings.HasSuffix(url, ".yaml") || strings.HasSuffix(url, ".yml") {
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
	if schema.Channels == nil {
		log.Printf("Warning: AsyncAPI schema has no channels defined.")
	}

	// --- Add Logging ---
	log.Println("DEBUG: AsyncAPI schema loaded successfully into structured format.")
	if schema.Components.Messages != nil {
		log.Printf("DEBUG [LoadFromJSON]: Inspecting Components.Messages:")
		for msgName, msg := range schema.Components.Messages {
			// Log the payload map for each message in components
			log.Printf("DEBUG [LoadFromJSON]:   Message '%s' -> Payload: %+v", msgName, msg.Payload)
		}
	} else {
		log.Printf("DEBUG [LoadFromJSON]: Components.Messages is nil.")
	}
	// --- End Logging ---

	l.schema = &schema // Assign to the loader
	return nil
}
