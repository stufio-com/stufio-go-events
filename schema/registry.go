package schema

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/stufio-com/stufio-go-events/messages"
	"github.com/xeipuuv/gojsonschema"
)

// SchemaRegistry manages AsyncAPI schemas and validation
type SchemaRegistry struct {
	mu                sync.RWMutex
	loader            *SchemaLoader
	asyncAPIURL       string
	eventSchemas      map[string]*gojsonschema.Schema // map of entity.action -> JSON schema
	lastRefresh       time.Time
	refreshTime       time.Duration
	definitions       []messages.EventDefinition
	validationEnabled bool
}

// NewSchemaRegistry creates a new schema registry
func NewSchemaRegistry(asyncAPIURL string, refreshTime time.Duration, validationEnabled bool) *SchemaRegistry {
	registry := &SchemaRegistry{
		loader:            NewSchemaLoader(),
		asyncAPIURL:       asyncAPIURL,
		eventSchemas:      make(map[string]*gojsonschema.Schema),
		refreshTime:       refreshTime,
		validationEnabled: validationEnabled,
	}

	// Initial schema load
	if err := registry.RefreshSchemas(); err != nil {
		log.Printf("Warning: Failed to load initial schemas: %v", err)
	}

	// Start periodic refresh if configured
	if refreshTime > 0 {
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

// RefreshSchemas loads and parses schemas from the AsyncAPI URL
func (r *SchemaRegistry) RefreshSchemas() error {
	log.Printf("Refreshing AsyncAPI schemas from %s", r.asyncAPIURL)

	if err := r.loader.LoadFromURL(r.asyncAPIURL); err != nil {
		return fmt.Errorf("loading AsyncAPI schema: %w", err)
	}

	// Extract event definitions
	definitions, err := r.loader.ExtractEventDefinitions()
	if err != nil {
		return fmt.Errorf("extracting event definitions: %w", err)
	}

	// Extract and compile payload schemas
	schemas, err := r.loader.ExtractPayloadSchemas()
	if err != nil {
		return fmt.Errorf("extracting payload schemas: %w", err)
	}

	// Update registry
	r.mu.Lock()
	defer r.mu.Unlock()

	r.definitions = definitions
	r.eventSchemas = schemas
	r.lastRefresh = time.Now()

	log.Printf("AsyncAPI schema refreshed: %d event definitions, %d payload schemas",
		len(definitions), len(schemas))

	return nil
}

// ValidatePayload validates a payload against its schema
func (r *SchemaRegistry) ValidatePayload(entityType, action string, payload interface{}) error {
	if !r.validationEnabled {
		return nil
	}

	r.mu.RLock()
	schema, exists := r.eventSchemas[fmt.Sprintf("%s.%s", entityType, action)]
	r.mu.RUnlock()

	if !exists {
		return fmt.Errorf("no schema found for %s.%s", entityType, action)
	}

	// Convert payload to JSON
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling payload: %w", err)
	}

	// Load document for validation
	documentLoader := gojsonschema.NewBytesLoader(payloadJSON)

	// Validate
	result, err := schema.Validate(documentLoader)
	if err != nil {
		return fmt.Errorf("validating payload: %w", err)
	}

	if !result.Valid() {
		var errorMessages []string
		for _, err := range result.Errors() {
			errorMessages = append(errorMessages, err.String())
		}
		return fmt.Errorf("payload validation failed: %v", errorMessages)
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
