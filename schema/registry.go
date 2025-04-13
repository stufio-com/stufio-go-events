package schema

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http" // Import strings
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
		payloadSchemaRefs: make(map[string]string), // Initialize ref map
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

	// Load the raw schema data first
	rawSchemaData, err := r.loader.fetchSchemaData(r.asyncAPIURL)
	if err != nil {
		log.Printf("Warning: Failed fetching schema data from %s: %v", r.asyncAPIURL, err)
		return fmt.Errorf("fetching schema data: %w", err)
	}

	// Parse the raw data into the full schema map
	var fullSchemaMap map[string]interface{}
	if err := json.Unmarshal(rawSchemaData, &fullSchemaMap); err != nil {
		// Make this error fatal for the refresh operation
		log.Printf("ERROR: Failed unmarshalling full schema map from %s: %v", r.asyncAPIURL, err)
		return fmt.Errorf("unmarshalling full schema map: %w", err) // Return error
	}

	// --- Log top-level keys for debugging ---
	var keys []string
	for k := range fullSchemaMap {
		keys = append(keys, k)
	}
	log.Printf("DEBUG: Top-level keys in fetched schema: %v", keys)
	// --- End Debug ---

	// Now load it into the structured loader
	if err := r.loader.LoadFromJSON(rawSchemaData); err != nil {
		// Make this error fatal too
		log.Printf("ERROR: Failed loading AsyncAPI schema structure from %s: %v", r.asyncAPIURL, err)
		return fmt.Errorf("loading AsyncAPI schema structure: %w", err) // Return error
	}

	// Extract event definitions
	definitions, errDefs := r.loader.ExtractEventDefinitions()
	if errDefs != nil {
		log.Printf("Warning: Error extracting event definitions: %v", errDefs)
	}

	// Extract payload schemas (raw maps) AND their original $refs
	// Modify ExtractPayloadSchemas to return refs as well
	_, payloadRefs, errPayloads := r.loader.ExtractPayloadSchemasAndRefs() // <-- Need to implement this
	if errPayloads != nil {
		log.Printf("Warning: Error extracting payload schemas: %v", errPayloads)
	}

	// Update registry
	r.mu.Lock()
	defer r.mu.Unlock()

	if errDefs == nil {
		r.definitions = definitions
	}
	r.rawFullSchema = fullSchemaMap
	r.payloadSchemaRefs = payloadRefs // Store the refs

	compiledSchemas := make(map[string]*gojsonschema.Schema)
	schemaLoaderWithContext := gojsonschema.NewSchemaLoader()
	// Add the full schema document with a base URI to allow resolving #/components/...
	errAdd := schemaLoaderWithContext.AddSchema("asyncapi", gojsonschema.NewGoLoader(r.rawFullSchema))
	if errAdd != nil {
		log.Printf("Error adding full schema to validator context: %v", errAdd)
		// Cannot proceed with compilation if context fails
		r.eventSchemas = make(map[string]*gojsonschema.Schema) // Clear potentially stale schemas
		return fmt.Errorf("adding full schema context: %w", errAdd)
	}
	r.fullSchemaValidator = schemaLoaderWithContext // Store the loader

	// Compile each specific payload schema using the context loader
	for key, payloadRef := range r.payloadSchemaRefs {
		// --- Add Debug Log ---
		log.Printf("DEBUG: Attempting to compile schema for key '%s' using ref: '%s'", key, payloadRef)
		// --- End Debug Log ---

		// Use the loader (which contains the full schema) to compile the specific ref
		// The reference itself should be just "#/components/schemas/..."
		refLoader := gojsonschema.NewReferenceLoader(payloadRef)          // Use the original ref
		compiledSchema, err := schemaLoaderWithContext.Compile(refLoader) // Compile using the loader that has the full schema context
		if err != nil {
			// Log the specific ref that failed
			log.Printf("Error compiling schema for %s (ref: %s): %v. Check schema structure and $refs.", key, payloadRef, err)
			continue // Skip this schema, but try others
		}
		compiledSchemas[key] = compiledSchema
	}

	r.eventSchemas = compiledSchemas // Update compiled schemas map
	r.lastRefresh = time.Now()
	log.Printf("AsyncAPI schema refreshed: %d event definitions, %d payload schemas compiled (check warnings for errors)",
		len(r.definitions), len(compiledSchemas))

	return nil // Return nil even if only parts failed, rely on logs
}

// ValidatePayload validates a payload against its schema
func (r *SchemaRegistry) ValidatePayload(entityType, action string, payload interface{}) error {
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

// --- Add helper to fetch raw schema data ---
// (Ensure this or similar logic exists)
func (l *SchemaLoader) fetchSchemaData(url string) ([]byte, error) {
	// ... (Implementation as provided before) ...
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetching schema from URL %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error fetching schema %s: %s", url, resp.Status)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading schema response from %s: %w", url, err)
	}
	return data, nil
}
