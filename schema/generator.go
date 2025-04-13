package schema

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/stufio-com/stufio-go-events/messages"
	"github.com/xeipuuv/gojsonschema"
)

// Generator handles code generation from AsyncAPI schemas
type Generator struct {
	packageName      string
	verbose          bool
	types            map[string]string
	imports          map[string]bool
	generatedTypes   map[string]bool
	entityTypeFilter []string // New field
	includePatterns  []string // New field
	excludePatterns  []string // New field
}

// NewGenerator creates a new code generator
func NewGenerator(packageName string) *Generator {
	return &Generator{
		packageName:    packageName,
		verbose:        false,
		types:          make(map[string]string),
		imports:        make(map[string]bool),
		generatedTypes: make(map[string]bool),
	}
}

// SetVerbose enables or disables verbose output
func (g *Generator) SetVerbose(verbose bool) {
	g.verbose = verbose
}

// FilterByEntityTypes sets entity types to include in generation
func (g *Generator) FilterByEntityTypes(entityTypes []string) {
	g.entityTypeFilter = entityTypes
}

// SetIncludePatterns sets patterns of entity.action to include in generation
func (g *Generator) SetIncludePatterns(patterns []string) {
	g.includePatterns = patterns
}

// SetExcludePatterns sets patterns of entity.action to exclude from generation
func (g *Generator) SetExcludePatterns(patterns []string) {
	g.excludePatterns = patterns
}

// matchesPattern checks if an event name matches a wildcard pattern
func (g *Generator) matchesPattern(name, pattern string) bool {
	if pattern == "*" || pattern == name {
		return true
	}

	patternParts := strings.Split(pattern, ".")
	nameParts := strings.Split(name, ".")

	if len(patternParts) != len(nameParts) {
		return false
	}

	for i, part := range patternParts {
		if part == "*" {
			continue
		}
		if strings.HasSuffix(part, "*") && strings.HasPrefix(nameParts[i], part[:len(part)-1]) {
			continue
		}
		if strings.HasPrefix(part, "*") && strings.HasSuffix(nameParts[i], part[1:]) {
			continue
		}
		if part != nameParts[i] {
			return false
		}
	}

	return true
}

// shouldIncludeEvent determines if an event should be included based on filters
func (g *Generator) shouldIncludeEvent(entityType, action string) bool {
	eventName := fmt.Sprintf("%s.%s", entityType, action)

	// Check entity type filter
	if len(g.entityTypeFilter) > 0 {
		included := false
		for _, et := range g.entityTypeFilter {
			if et == entityType {
				included = true
				break
			}
		}
		if !included {
			return false
		}
	}

	// Check exclude patterns
	for _, pattern := range g.excludePatterns {
		if g.matchesPattern(eventName, pattern) {
			return false
		}
	}

	// Check include patterns
	if len(g.includePatterns) > 0 {
		for _, pattern := range g.includePatterns {
			if g.matchesPattern(eventName, pattern) {
				return true
			}
		}
		return false
	}

	return true
}

// GenerateStructs generates Go structs from the provided schemas
func (g *Generator) GenerateStructs(schemas map[string]*gojsonschema.Schema, events []messages.EventDefinition, outputDir string) ([]string, error) {
	var files []string

	// Filter events based on configured filters
	var filteredEvents []messages.EventDefinition
	for _, event := range events {
		if g.shouldIncludeEvent(event.EntityType, event.Action) {
			filteredEvents = append(filteredEvents, event)
		} else if g.verbose {
			fmt.Printf("Skipping event: %s.%s (filtered out)\n", event.EntityType, event.Action)
		}
	}

	// Filter schemas based on filtered events
	filteredSchemas := make(map[string]*gojsonschema.Schema)
	for name, schema := range schemas {
		parts := strings.Split(name, ".")
		if len(parts) >= 2 {
			entityType := parts[0]
			action := parts[1]

			if g.shouldIncludeEvent(entityType, action) {
				filteredSchemas[name] = schema
			}
		}
	}

	// Generate event types file only for filtered events
	eventTypesFile, err := g.generateEventTypes(filteredEvents, outputDir)
	if err != nil {
		return nil, fmt.Errorf("generating event types: %w", err)
	}
	files = append(files, eventTypesFile)

	// Process each schema in filtered schemas
	for name := range filteredSchemas { // Renamed variable to avoid shadowing
		// Parse entity and action from the name
		parts := strings.Split(name, ".")
		if len(parts) < 2 {
			if g.verbose {
				fmt.Printf("Skipping schema with invalid name: %s\n", name)
			}
			continue
		}

		entity := parts[0]
		action := parts[1]

		// Generate file for this entity if it doesn't exist yet
		if !g.generatedTypes[entity] {
			fileName, err := g.generateEntityFile(entity, filteredSchemas, filteredEvents, outputDir)
			if err != nil {
				return nil, fmt.Errorf("generating entity file for %s: %w", entity, err)
			}
			files = append(files, fileName)
			g.generatedTypes[entity] = true
		}

		if g.verbose {
			fmt.Printf("Processed schema: %s.%s\n", entity, action)
		}
	}

	return files, nil
}

// generateEventTypes generates a file with event type constants
func (g *Generator) generateEventTypes(events []messages.EventDefinition, outputDir string) (string, error) {
	// Group events by entity type
	eventsByEntity := make(map[string][]messages.EventDefinition)
	for _, event := range events {
		eventsByEntity[event.EntityType] = append(eventsByEntity[event.EntityType], event)
	}

	// Prepare template data
	var entityTypes []string
	for entity := range eventsByEntity {
		entityTypes = append(entityTypes, entity)
	}
	sort.Strings(entityTypes)

	templateData := map[string]interface{}{
		"Package":        g.packageName,
		"Timestamp":      time.Now().Format(time.RFC3339),
		"EntityTypes":    entityTypes,
		"EventsByEntity": eventsByEntity,
	}

	// Create the template
	tmpl, err := template.New("eventTypes").Parse(eventTypesTemplate)
	if err != nil {
		return "", fmt.Errorf("parsing template: %w", err)
	}

	// Execute the template
	var buffer bytes.Buffer
	if err := tmpl.Execute(&buffer, templateData); err != nil {
		return "", fmt.Errorf("executing template: %w", err)
	}

	// Format the generated code
	formattedCode, err := format.Source(buffer.Bytes())
	if err != nil {
		return "", fmt.Errorf("formatting code: %w", err)
	}

	// Write to file
	fileName := filepath.Join(outputDir, "event_types.go")
	if err := os.WriteFile(fileName, formattedCode, 0644); err != nil {
		return "", fmt.Errorf("writing file: %w", err)
	}

	return fileName, nil
}

// generateEntityFile generates a file for an entity with all its event payloads
func (g *Generator) generateEntityFile(entityType string, schemas map[string]*gojsonschema.Schema, events []messages.EventDefinition, outputDir string) (string, error) {
	// Reset imports for this file
	g.imports = make(map[string]bool)

	// Find all schemas for this entity
	var entitySchemas []struct {
		Name   string
		Action string
		Schema *gojsonschema.Schema
	}

	for name, schema := range schemas {
		parts := strings.Split(name, ".")
		if len(parts) >= 2 && parts[0] == entityType {
			entitySchemas = append(entitySchemas, struct {
				Name   string
				Action string
				Schema *gojsonschema.Schema
			}{
				Name:   name,
				Action: parts[1],
				Schema: schema,
			})
		}
	}

	if len(entitySchemas) == 0 {
		if g.verbose {
			fmt.Printf("No schemas found for entity: %s\n", entityType)
		}
		return "", nil
	}

	// Sort schemas by action name for consistency
	sort.Slice(entitySchemas, func(i, j int) bool {
		return entitySchemas[i].Action < entitySchemas[j].Action
	})

	// Get event definitions for this entity
	var entityEvents []messages.EventDefinition
	for _, event := range events {
		if event.EntityType == entityType {
			entityEvents = append(entityEvents, event)
		}
	}

	// Generate struct definitions
	structs := []string{}
	imports := []string{}

	for _, schema := range entitySchemas {
		structDef, requiredImports, err := g.generateStructForSchema(entityType, schema.Action, schema.Schema)
		if err != nil {
			return "", fmt.Errorf("generating struct for %s.%s: %w", entityType, schema.Action, err)
		}

		structs = append(structs, structDef)

		// Add required imports
		for imp := range requiredImports {
			g.imports[imp] = true
		}
	}

	// Prepare imports for template
	for imp := range g.imports {
		imports = append(imports, imp)
	}
	sort.Strings(imports)

	// Prepare template data
	templateData := map[string]interface{}{
		"Package":    g.packageName,
		"Timestamp":  time.Now().Format(time.RFC3339),
		"EntityType": entityType,
		"StructName": pascalCase(entityType),
		"Structs":    structs,
		"Imports":    imports,
		"Events":     entityEvents,
	}

	// Create the template
	tmpl, err := template.New("entityFile").Parse(entityFileTemplate)
	if err != nil {
		return "", fmt.Errorf("parsing template: %w", err)
	}

	// Execute the template
	var buffer bytes.Buffer
	if err := tmpl.Execute(&buffer, templateData); err != nil {
		return "", fmt.Errorf("executing template: %w", err)
	}

	// Format the generated code
	formattedCode, err := format.Source(buffer.Bytes())
	if err != nil {
		// If formatting fails, still write the unformatted code for debugging
		if g.verbose {
			fmt.Printf("Warning: Failed to format code for %s: %v\n", entityType, err)
			fmt.Printf("Writing unformatted code instead\n")
		}
		formattedCode = buffer.Bytes()
	}

	// Create file name with snake case
	fileName := filepath.Join(outputDir, snakeCase(entityType)+"_events.go")

	// Write to file
	if err := os.WriteFile(fileName, formattedCode, 0644); err != nil {
		return "", fmt.Errorf("writing file: %w", err)
	}

	return fileName, nil
}

// generateStructForSchema generates a Go struct definition from a JSON schema
func (g *Generator) generateStructForSchema(entityType, action string, schema *gojsonschema.Schema) (string, map[string]bool, error) {
	// We need to adapt this to work with gojsonschema.Schema
	// Use the raw document object directly from the schema
	var rawSchema map[string]interface{}

	// Try to extract schema properties
	// Since gojsonschema.Schema doesn't expose its internal JSON structure directly,
	// we need to infer properties from what we can access
	rawSchema = extractSchemaProperties(schema)

	structName := pascalCase(action) + "Payload"

	// Start building the struct
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("// %s represents the payload for %s.%s events\n", structName, entityType, action))
	buffer.WriteString(fmt.Sprintf("type %s struct {\n", structName))

	// Track required imports
	requiredImports := make(map[string]bool)

	// Extract properties
	properties, ok := rawSchema["properties"].(map[string]interface{})
	if !ok {
		// If no properties defined, create an empty struct
		buffer.WriteString("}\n\n")
		return buffer.String(), requiredImports, nil
	}

	// Get required fields
	requiredFields := make(map[string]bool)
	if required, ok := rawSchema["required"].([]interface{}); ok {
		for _, field := range required {
			if fieldName, ok := field.(string); ok {
				requiredFields[fieldName] = true
			}
		}
	}

	// Sort property names for consistent output
	var propertyNames []string
	for name := range properties {
		propertyNames = append(propertyNames, name)
	}
	sort.Strings(propertyNames)

	// Generate field for each property
	for _, name := range propertyNames {
		propSchema, ok := properties[name].(map[string]interface{})
		if !ok {
			continue
		}

		fieldName := pascalCase(name)
		fieldType, imports := g.jsonSchemaToGoType(propSchema)

		// Add imports
		for imp := range imports {
			requiredImports[imp] = true
		}

		// Generate field description
		if description, ok := propSchema["description"].(string); ok && description != "" {
			buffer.WriteString(fmt.Sprintf("\t// %s\n", description))
		}

		// Generate field with json tag
		jsonTag := name
		if !requiredFields[name] {
			jsonTag += ",omitempty"
		}
		buffer.WriteString(fmt.Sprintf("\t%s %s `json:\"%s\"`\n", fieldName, fieldType, jsonTag))
	}

	buffer.WriteString("}\n\n")

	return buffer.String(), requiredImports, nil
}

// jsonSchemaToGoType converts a JSON schema type to a Go type
func (g *Generator) jsonSchemaToGoType(schema map[string]interface{}) (string, map[string]bool) {
	imports := make(map[string]bool)

	// Handle references
	if ref, ok := schema["$ref"].(string); ok {
		// Extract type name from reference
		parts := strings.Split(ref, "/")
		typeName := parts[len(parts)-1]
		return pascalCase(typeName), imports
	}

	// Get the type
	schemaType, _ := schema["type"].(string)

	// Handle arrays
	if schemaType == "array" {
		items, ok := schema["items"].(map[string]interface{})
		if !ok {
			return "[]interface{}", imports
		}

		itemType, itemImports := g.jsonSchemaToGoType(items)
		for imp := range itemImports {
			imports[imp] = true
		}

		return "[]" + itemType, imports
	}

	// Handle objects
	if schemaType == "object" {
		if additionalProps, ok := schema["additionalProperties"].(map[string]interface{}); ok {
			valueType, valueImports := g.jsonSchemaToGoType(additionalProps)
			for imp := range valueImports {
				imports[imp] = true
			}

			return fmt.Sprintf("map[string]%s", valueType), imports
		}

		// Handle nested objects
		return "map[string]interface{}", imports
	}

	// Handle primitive types
	switch schemaType {
	case "string":
		// Check if it's a date-time format
		if format, ok := schema["format"].(string); ok {
			if format == "date-time" {
				imports["time"] = true
				return "time.Time", imports
			}
		}
		return "string", imports
	case "integer":
		return "int64", imports
	case "number":
		return "float64", imports
	case "boolean":
		return "bool", imports
	case "null":
		return "interface{}", imports
	default:
		// For unknown or empty types, default to interface{}
		return "interface{}", imports
	}
}

// Helper function to extract schema properties
func extractSchemaProperties(schema *gojsonschema.Schema) map[string]interface{} {
	// This is a simple placeholder that returns a basic schema structure
	// In a real implementation, you'd use reflection or other methods to access
	// the internal schema definition
	return map[string]interface{}{
		"type":       "object",
		"properties": map[string]interface{}{},
		"required":   []interface{}{},
	}
}

// Helper functions for case conversions

// pascalCase converts a string to PascalCase
func pascalCase(s string) string {
	// Handle special cases
	if s == "id" || s == "Id" {
		return "ID"
	}

	// Split by underscore, dash or space
	parts := regexp.MustCompile(`[_\- ]`).Split(s, -1)
	for i, part := range parts {
		if len(part) > 0 {
			parts[i] = strings.ToUpper(part[0:1]) + strings.ToLower(part[1:])
		}
	}

	return strings.Join(parts, "")
}

// snakeCase converts a string to snake_case
func snakeCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 && ('A' <= r && r <= 'Z') {
			result.WriteByte('_')
		}
		result.WriteRune(r)
	}
	return strings.ToLower(result.String())
}

// Templates for code generation
const eventTypesTemplate = `// Code generated by eventgen; DO NOT EDIT.
// Generated at: {{.Timestamp}}

package {{.Package}}

// Event types constants
const (
    {{- range $entity := .EntityTypes}}
    // {{pascalCase $entity}}Events
    {{- range $event := index $.EventsByEntity $entity}}
    Event{{pascalCase $entity}}{{pascalCase $event.Action}} = "{{$entity}}.{{$event.Action}}"
    {{- end}}
    {{end}}
)
`

const entityFileTemplate = `// Code generated by eventgen; DO NOT EDIT.
// Generated at: {{.Timestamp}}

package {{.Package}}

import (
    {{- range $import := .Imports}}
    "{{$import}}"
    {{- end}}
)

{{range .Structs}}
{{.}}
{{end}}
`
