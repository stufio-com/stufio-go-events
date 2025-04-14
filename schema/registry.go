package schema

import (
	"encoding/json"
	"fmt"
	"log" // Import strings
	"sync"
	"time"

	"github.com/stufio-com/stufio-go-events/messages"
	"github.com/xeipuuv/gojsonschema"
)

// SchemaRegistry manages AsyncAPI schemas and validation
type SchemaRegistry struct {
	mu                  sync.RWMutex
	loader              *SchemaLoader
	asyncAPIURL         string
	eventSchemas        map[string]*gojsonschema.Schema // map of entity.action -> JSON schema
	payloadSchemaRefs   map[string]string               // map of entity.action -> original $ref string for payload
	lastRefresh         time.Time
	refreshTime         time.Duration
	definitions         []messages.EventDefinition
	validationEnabled   bool
	rawFullSchema       map[string]interface{}     // Store the full raw schema
	fullSchemaValidator *gojsonschema.SchemaLoader // Loader with full schema context
}

// NewSchemaRegistry creates a new schema registry
func NewSchemaRegistry(asyncAPIURL string, refreshTime time.Duration, validationEnabled bool) *SchemaRegistry {
	registry := &SchemaRegistry{
		loader:            NewSchemaLoader(),
		asyncAPIURL:       asyncAPIURL,
		eventSchemas:      make(map[string]*gojsonschema.Schema),
		payloadSchemaRefs: make(map[string]string),
		refreshTime:       refreshTime,
		validationEnabled: validationEnabled,
	}

	// Only attempt schema load if validation is enabled
	if validationEnabled {
		if err := registry.RefreshSchemas(); err != nil {
			log.Printf("Warning: Failed to load initial schemas: %v", err)
		}
	} else {
		log.Printf("Schema validation disabled, skipping initial schema load.")
	}

	// Start periodic refresh if configured and validation is enabled
	if refreshTime > 0 && validationEnabled {
		go registry.periodicRefresh()
	}

	return registry
}

// periodicRefresh periodically refreshes schemas from the URL
func (r *SchemaRegistry) periodicRefresh() {
	ticker := time.NewTicker(r.refreshTime)
	defer ticker.Stop()

	for {
		<-ticker.C
		if err := r.RefreshSchemas(); err != nil {
			log.Printf("Error refreshing schemas: %v", err)
		}
	}
}

// ValidatePayload validates a payload against its schema
func (r *SchemaRegistry) ValidatePayload(entityType, action string, payload interface{}) error {
	// Early return if validation is disabled
	if !r.validationEnabled {
		return nil
	}

	key := fmt.Sprintf("%s.%s", entityType, action)

	r.mu.RLock()
	// Get the specifically compiled schema for this event type
	compiledSchema, exists := r.eventSchemas[key]
	r.mu.RUnlock()

	if !exists {
		// log.Printf("Warning: No schema found for %s to validate payload.", key)
		return nil // Don't fail if schema missing
	}

	// Convert payload to JSON
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling payload for validation: %w", err)
	}
	documentLoader := gojsonschema.NewBytesLoader(payloadJSON)

	// Validate using the specific compiled schema
	result, err := compiledSchema.Validate(documentLoader)
	if err != nil {
		// This error is less likely now if compilation succeeded
		return fmt.Errorf("validating payload for %s: %w", key, err)
	}

	if !result.Valid() {
		var errorMessages []string
		for _, desc := range result.Errors() {
			errorMessages = append(errorMessages, desc.String())
		}
		log.Printf("Payload validation failed for %s: %v", key, errorMessages) // Log details
		return fmt.Errorf("payload validation failed for %s", key)             // Return simpler error
	}

	return nil
}

// GetEventDefinition gets an event definition by entity type and action
func (r *SchemaRegistry) GetEventDefinition(entityType, action string) (messages.EventDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, def := range r.definitions {
		if def.EntityType == entityType && def.Action == action {
			return def, true
		}
	}

	return messages.EventDefinition{}, false
}

// GetEventDefinitions gets all event definitions
func (r *SchemaRegistry) GetEventDefinitions() []messages.EventDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Return a copy to avoid concurrent modification issues
	result := make([]messages.EventDefinition, len(r.definitions))
	copy(result, r.definitions)
	return result
}

// Global instance
var globalRegistry *SchemaRegistry
var initOnce sync.Once

// InitGlobalRegistry initializes the global schema registry
func InitGlobalRegistry(asyncAPIURL string, refreshTime time.Duration, validationEnabled bool) {
	initOnce.Do(func() {
		globalRegistry = NewSchemaRegistry(asyncAPIURL, refreshTime, validationEnabled)
	})
}

// GetGlobalRegistry returns the global schema registry
func GetGlobalRegistry() *SchemaRegistry {
	if globalRegistry == nil {
		panic("Schema registry not initialized. Call InitGlobalRegistry first")
	}
	return globalRegistry
}
