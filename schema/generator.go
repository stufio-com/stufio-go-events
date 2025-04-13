package schema

import (
	"bytes"
	"fmt"
	"go/format"
	"log" // Import log package
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/stufio-com/stufio-go-events/messages"
	// Remove gojsonschema import if no longer needed here
)

// Generator handles code generation from AsyncAPI schemas
type Generator struct {
	packageName      string
	verbose          bool
	types            map[string]string
	imports          map[string]bool
	generatedTypes   map[string]bool                   // Tracks generated structs
	enumsToGenerate  map[string]map[string]interface{} // Tracks enums to generate [TypeName]SchemaMap
	entityTypeFilter []string
	includePatterns  []string
	excludePatterns  []string
	schemaComponents map[string]interface{} // Store components.schemas for resolving internal refs
}

// NewGenerator creates a new code generator
func NewGenerator(packageName string, schemaComponents map[string]interface{}) *Generator { // Accept schema components
	return &Generator{
		packageName:      packageName,
		verbose:          false,
		types:            make(map[string]string),
		imports:          make(map[string]bool),
		generatedTypes:   make(map[string]bool),
		enumsToGenerate:  make(map[string]map[string]interface{}), // Initialize map
		schemaComponents: schemaComponents,                        // Store components
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
// Accepts map[string]map[string]interface{} for schemas
func (g *Generator) GenerateStructs(schemas map[string]map[string]interface{}, events []messages.EventDefinition, outputDir string) ([]string, error) {
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
	filteredSchemas := make(map[string]map[string]interface{}) // Use correct type
	for name, schema := range schemas {
		parts := strings.Split(name, ".")
		if len(parts) >= 2 {
			entityType := parts[0]
			action := parts[1]

			if g.shouldIncludeEvent(entityType, action) {
				filteredSchemas[name] = schema // Store the map
			}
		}
	}

	// --- Pre-process schemas to find all enums ---
	// Iterate through all filtered schemas and their properties to populate g.enumsToGenerate
	// This ensures enums are detected even if not directly used by a filtered event's top-level payload
	// (though the current logic in jsonSchemaToGoType called during entity file generation should cover most cases)
	// For simplicity, we rely on jsonSchemaToGoType called during entity generation to populate the map.

	// Generate event types file only for filtered events
	eventTypesFile, err := g.generateEventTypes(filteredEvents, outputDir)
	if err != nil {
		return nil, fmt.Errorf("generating event types: %w", err)
	}
	if eventTypesFile != "" { // Check if file was actually generated
		files = append(files, eventTypesFile)
	}

	// Process each schema in filtered schemas to generate entity files
	// This process will populate g.enumsToGenerate via calls to jsonSchemaToGoType
	processedEntities := make(map[string]bool) // Track processed entities to generate file once
	for name := range filteredSchemas {
		// Parse entity and action from the name
		parts := strings.Split(name, ".")
		if len(parts) < 2 {
			if g.verbose {
				fmt.Printf("Skipping schema with invalid name: %s\n", name)
			}
			continue
		}
		entity := parts[0]

		// Generate file for this entity if it hasn't been processed yet
		if !processedEntities[entity] {
			fileName, err := g.generateEntityFile(entity, filteredSchemas, filteredEvents, outputDir)
			if err != nil {
				log.Printf("Error generating entity file for %s: %v", entity, err)
			} else if fileName != "" {
				files = append(files, fileName)
			}
			processedEntities[entity] = true
		}
		// Note: g.enumsToGenerate is populated during generateEntityFile -> generateStructForSchema -> jsonSchemaToGoType
	}

	// --- Generate common types file AFTER processing entities ---
	commonTypesFile, err := g.generateCommonTypesFile(outputDir)
	if err != nil {
		// Log error but potentially continue? Or make it fatal? Let's make it fatal for now.
		return files, fmt.Errorf("generating common types file: %w", err)
	}
	if commonTypesFile != "" { // Check if file was actually generated
		files = append(files, commonTypesFile)
	}

	return files, nil
}

// generateEventTypes generates a file with event type constants
func (g *Generator) generateEventTypes(events []messages.EventDefinition, outputDir string) (string, error) {
	// Group events by entity type
	eventsByEntity := make(map[string][]messages.EventDefinition)
	for _, event := range events {
		// Sort events within each entity for consistent output
		entityEvents := eventsByEntity[event.EntityType]
		entityEvents = append(entityEvents, event)
		sort.Slice(entityEvents, func(i, j int) bool {
			return entityEvents[i].Action < entityEvents[j].Action
		})
		eventsByEntity[event.EntityType] = entityEvents
	}

	// Prepare template data
	var entityTypes []string
	for entity := range eventsByEntity {
		entityTypes = append(entityTypes, entity)
	}
	sort.Strings(entityTypes) // Sort entity types for consistent output

	templateData := map[string]interface{}{
		"Package":        g.packageName,
		"Timestamp":      time.Now().Format(time.RFC3339),
		"EntityTypes":    entityTypes,
		"EventsByEntity": eventsByEntity,
	}

	// Define template functions
	funcMap := template.FuncMap{
		"pascalCase": pascalCase, // Add pascalCase function to the map
	}

	// Create the template and add the function map
	tmpl, err := template.New("eventTypes").Funcs(funcMap).Parse(eventTypesTemplate) // Add .Funcs(funcMap)
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
		// Log formatting error but return unformatted code for debugging
		log.Printf("Warning: Failed to format event_types.go: %v", err)
		formattedCode = buffer.Bytes()
		// return "", fmt.Errorf("formatting code: %w", err) // Don't fail on formatting error
	}

	// Write to file
	fileName := filepath.Join(outputDir, "event_types.go")
	if err := os.WriteFile(fileName, formattedCode, 0644); err != nil {
		return "", fmt.Errorf("writing file: %w", err)
	}

	return fileName, nil
}

// generateEntityFile generates a file for an entity with all its event payloads
func (g *Generator) generateEntityFile(entityType string, allSchemas map[string]map[string]interface{}, events []messages.EventDefinition, outputDir string) (string, error) {
	// Reset imports for this file
	g.imports = make(map[string]bool)

	// Find all schemas for this entity
	type entitySchemaInfo struct {
		Name      string
		Action    string
		SchemaMap map[string]interface{}
	}
	var entitySchemas []entitySchemaInfo

	for name, schemaMap := range allSchemas {
		parts := strings.Split(name, ".")
		if len(parts) >= 2 && parts[0] == entityType {
			entitySchemas = append(entitySchemas, entitySchemaInfo{
				Name:      name,
				Action:    parts[1],
				SchemaMap: schemaMap,
			})
		}
	}

	if len(entitySchemas) == 0 {
		if g.verbose {
			fmt.Printf("No schemas found for entity: %s\n", entityType)
		}
		return "", nil // Return empty string, not an error
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

	for _, schemaInfo := range entitySchemas {
		// Pass the schema map directly
		structDef, requiredImports, err := g.generateStructForSchema(entityType, schemaInfo.Action, schemaInfo.SchemaMap)
		if err != nil {
			// Log error but continue generating other structs for this entity
			log.Printf("Error generating struct for %s.%s: %v", entityType, schemaInfo.Action, err)
			continue // Skip this struct
		}

		structs = append(structs, structDef)

		// Add required imports
		for imp := range requiredImports {
			g.imports[imp] = true
		}
	}

	// If no structs were successfully generated, don't create the file
	if len(structs) == 0 {
		log.Printf("No structs generated for entity file: %s", entityType)
		return "", nil
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
		"StructName": pascalCase(entityType), // This might not be used if structs are named per action
		"Structs":    structs,
		"Imports":    imports,
		"Events":     entityEvents, // Keep events for potential future use in template
	}

	// Create the template
	tmpl, err := template.New("entityFile").Parse(entityFileTemplate)
	if err != nil {
		return "", fmt.Errorf("parsing entity template: %w", err)
	}

	// Execute the template
	var buffer bytes.Buffer
	if err := tmpl.Execute(&buffer, templateData); err != nil {
		return "", fmt.Errorf("executing entity template: %w", err)
	}

	// Format the generated code
	formattedCode, err := format.Source(buffer.Bytes())
	if err != nil {
		// If formatting fails, still write the unformatted code for debugging
		if g.verbose {
			fmt.Printf("Warning: Failed to format code for %s: %v\n", entityType, err)
			fmt.Printf("Writing unformatted code instead\n")
		}
		formattedCode = buffer.Bytes() // Use unformatted code on error
	}

	// Create file name with snake case
	fileName := filepath.Join(outputDir, snakeCase(entityType)+"_events.go")

	// Write to file
	if err := os.WriteFile(fileName, formattedCode, 0644); err != nil {
		return "", fmt.Errorf("writing entity file %s: %w", fileName, err)
	}

	return fileName, nil
}

// generateStructForSchema generates a Go struct definition from a JSON schema map
func (g *Generator) generateStructForSchema(entityType, action string, schemaMap map[string]interface{}) (string, map[string]bool, error) {
	structName := pascalCase(action) + "Payload"

	// Start building the struct
	var buffer bytes.Buffer
	// Add description from schema if available
	if description, ok := schemaMap["description"].(string); ok && description != "" {
		buffer.WriteString(fmt.Sprintf("// %s: %s\n", structName, description))
	} else {
		buffer.WriteString(fmt.Sprintf("// %s represents the payload for %s.%s events\n", structName, entityType, action))
	}
	buffer.WriteString(fmt.Sprintf("type %s struct {\n", structName))

	// Track required imports
	requiredImports := make(map[string]bool)

	// Extract properties
	properties, ok := schemaMap["properties"].(map[string]interface{})
	if !ok {
		// If no properties defined (e.g., empty object schema), create an empty struct
		log.Printf("Warning: No properties found for schema %s.%s", entityType, action)
		buffer.WriteString("}\n\n")
		return buffer.String(), requiredImports, nil
	}

	// Get required fields
	requiredFields := make(map[string]bool)
	if required, ok := schemaMap["required"].([]interface{}); ok {
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
			log.Printf("Warning: Property '%s' in schema %s.%s is not a valid object", name, entityType, action)
			continue
		}

		fieldName := pascalCase(name)
		// Pass the property schema map
		fieldType, imports := g.jsonSchemaToGoType(propSchema)

		// Add imports
		for imp := range imports {
			requiredImports[imp] = true
		}

		// Generate field description
		if description, ok := propSchema["description"].(string); ok && description != "" {
			// Simple multi-line comment handling
			lines := strings.Split(description, "\n")
			for _, line := range lines {
				buffer.WriteString(fmt.Sprintf("\t// %s\n", strings.TrimSpace(line)))
			}
		}

		// Generate field with json tag
		jsonTag := name
		// Handle omitempty - only add if NOT required AND type is nullable (pointer or slice/map)
		// Basic check: add omitempty if not required
		if !requiredFields[name] {
			jsonTag += ",omitempty"
		}
		buffer.WriteString(fmt.Sprintf("\t%s %s `json:\"%s\"`\n", fieldName, fieldType, jsonTag))
	}

	buffer.WriteString("}\n\n")

	return buffer.String(), requiredImports, nil
}

// jsonSchemaToGoType converts a JSON schema type to a Go type string
// It now also resolves internal $refs using g.schemaComponents
func (g *Generator) jsonSchemaToGoType(schema map[string]interface{}) (string, map[string]bool) {
	imports := make(map[string]bool)

	// Handle references first
	if ref, ok := schema["$ref"].(string); ok {
		typeName, err := g.resolveRefToTypeName(ref)
		if err != nil {
			log.Printf("Warning: Failed to resolve $ref '%s' to type name: %v", ref, err)
			return "interface{}", imports // Fallback for unresolved refs
		}

		// Check if the resolved type itself needs imports or needs generation (e.g., enum)
		resolvedSchema, lookupErr := g.lookupComponentSchema(ref)
		if lookupErr == nil {
			// Check if it's an enum that needs its own type generated
			if schemaType, ok := resolvedSchema["type"].(string); ok && schemaType == "string" {
				if _, hasEnum := resolvedSchema["enum"]; hasEnum {
					// It's an enum, mark it for generation if not already done
					if _, exists := g.enumsToGenerate[typeName]; !exists {
						g.enumsToGenerate[typeName] = resolvedSchema
						if g.verbose {
							log.Printf("Detected enum '%s' for generation.", typeName)
						}
					}
					// The type name remains the PascalCase name (e.g., Domainstatus)
					return typeName, imports
				}
			}

			// Check for time.Time specifically based on format
			if schemaType, ok := resolvedSchema["type"].(string); ok && schemaType == "string" {
				if format, ok := resolvedSchema["format"].(string); ok {
					if format == "date-time" || format == "date" {
						imports["time"] = true
						return "time.Time", imports
					}
				}
			}
			// Add checks for other simple types needing generation if necessary
		} else {
			log.Printf("Warning: Could not look up schema for $ref '%s': %v", ref, lookupErr)
		}

		// Return the resolved type name (e.g., UserCreatedPayload, Domainstatus)
		return typeName, imports
	}

	// Handle combined types like anyOf, oneOf (use interface{} for simplicity for now)
	if _, ok := schema["anyOf"]; ok {
		log.Printf("Warning: anyOf type found, using interface{}")
		return "interface{}", imports
	}
	if _, ok := schema["oneOf"]; ok {
		log.Printf("Warning: oneOf type found, using interface{}")
		return "interface{}", imports
	}
	if _, ok := schema["allOf"]; ok {
		// If allOf has a single $ref, handle it like a direct $ref
		allOfItems, ok := schema["allOf"].([]interface{})
		if ok && len(allOfItems) == 1 {
			if itemMap, ok := allOfItems[0].(map[string]interface{}); ok {
				if _, ok := itemMap["$ref"]; ok {
					return g.jsonSchemaToGoType(itemMap) // Recurse with the ref map
				}
			}
		}
		log.Printf("Warning: allOf type found, using interface{}")
		return "interface{}", imports
	}

	// Get the primary type
	schemaType := ""
	if st, ok := schema["type"]; ok {
		// Handle type potentially being an array ["string", "null"]
		if typeStr, ok := st.(string); ok {
			schemaType = typeStr
		} else if typeArr, ok := st.([]interface{}); ok && len(typeArr) > 0 {
			// Prioritize non-null type if present
			for _, t := range typeArr {
				if ts, ok := t.(string); ok && ts != "null" {
					schemaType = ts
					break
				}
			}
			if schemaType == "" { // If only "null" or empty array
				schemaType = "null"
			}
		}
	}

	// Handle arrays
	if schemaType == "array" {
		items, ok := schema["items"].(map[string]interface{})
		if !ok {
			// Array of unknown type
			return "[]interface{}", imports
		}

		itemType, itemImports := g.jsonSchemaToGoType(items) // Recurse for item type
		for imp := range itemImports {
			imports[imp] = true
		}

		return "[]" + itemType, imports
	}

	// Handle objects (inline definition or map)
	if schemaType == "object" {
		// Check for map-like objects (additionalProperties)
		if additionalProps, ok := schema["additionalProperties"]; ok {
			// Check if it's explicitly false (no additional properties allowed)
			if apBool, ok := additionalProps.(bool); ok && !apBool {
				// This implies a struct with defined properties only.
				// If properties are also defined, handle as struct (fall through).
				// If no properties, it's an empty struct essentially.
				// Let's assume if additionalProperties is false, we use the defined properties.
				// If additionalProperties is true or a schema, it's a map.
			} else if apSchema, ok := additionalProps.(map[string]interface{}); ok {
				// It's a map with specific value types
				valueType, valueImports := g.jsonSchemaToGoType(apSchema)
				for imp := range valueImports {
					imports[imp] = true
				}
				return fmt.Sprintf("map[string]%s", valueType), imports
			} else {
				// additionalProperties is true or not defined, default to map[string]interface{}
				return "map[string]interface{}", imports
			}
		}

		// If it's an object with properties but no additionalProperties schema,
		// it should ideally be represented by a generated struct.
		// However, jsonSchemaToGoType is called *within* struct generation.
		// This case implies a nested, anonymous struct. For simplicity, use map[string]interface{}.
		// Proper handling would require generating nested structs.
		if _, hasProps := schema["properties"]; hasProps {
			log.Printf("Warning: Nested anonymous object found, using map[string]interface{}")
		}
		return "map[string]interface{}", imports
	}

	// Handle primitive types
	switch schemaType {
	case "string":
		// Check format for specific types
		if format, ok := schema["format"].(string); ok {
			if format == "date-time" || format == "date" {
				imports["time"] = true
				return "time.Time", imports
			}
			if format == "uuid4" {
				// Often represented as string in Go
				return "string", imports
			}
			// Add other format handling if needed (e.g., email, uri -> string)
		}
		return "string", imports
	case "integer":
		// Use int64 for wider compatibility, especially with JSON numbers
		return "int64", imports
	case "number":
		return "float64", imports
	case "boolean":
		return "bool", imports
	case "null":
		// Represents a JSON null value, often mapped to interface{} or pointers in Go
		return "interface{}", imports // Or consider *struct{} ?
	default:
		// For unknown or empty types, default to interface{}
		if schemaType != "" {
			log.Printf("Warning: Unknown schema type '%s', using interface{}", schemaType)
		}
		return "interface{}", imports
	}
}

// Add helper to resolve $ref to type name
func (g *Generator) resolveRefToTypeName(ref string) (string, error) {
	if !strings.HasPrefix(ref, "#/components/schemas/") {
		return "", fmt.Errorf("unsupported $ref format for type resolution: %s", ref)
	}
	schemaName := strings.TrimPrefix(ref, "#/components/schemas/")

	// Convert schema name (e.g., UserCreatedPayload) to Go type name (e.g., UserCreatedPayload)
	// Assumes the referenced schema will also be generated as a struct if it's an object type.
	return pascalCase(schemaName), nil
}

// Add helper to look up the actual schema definition from components
func (g *Generator) lookupComponentSchema(ref string) (map[string]interface{}, error) {
	if !strings.HasPrefix(ref, "#/components/schemas/") {
		return nil, fmt.Errorf("unsupported $ref format for lookup: %s", ref)
	}
	schemaName := strings.TrimPrefix(ref, "#/components/schemas/")

	if g.schemaComponents == nil {
		return nil, fmt.Errorf("schema components not available in generator")
	}

	component, found := g.schemaComponents[schemaName]
	if !found {
		return nil, fmt.Errorf("schema '%s' not found in components", schemaName)
	}

	componentMap, ok := component.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("component schema '%s' is not a valid object", schemaName)
	}
	return componentMap, nil
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

// generateCommonTypesFile generates a file for common types like enums
func (g *Generator) generateCommonTypesFile(outputDir string) (string, error) {
	if len(g.enumsToGenerate) == 0 {
		return "", nil // No common types to generate
	}

	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf(`// Code generated by eventgen; DO NOT EDIT.
// Generated at: %s

package %s

`, time.Now().Format(time.RFC3339), g.packageName))

	// Sort enum names for consistent output
	var enumNames []string
	for name := range g.enumsToGenerate {
		enumNames = append(enumNames, name)
	}
	sort.Strings(enumNames)

	for _, enumName := range enumNames {
		schemaMap := g.enumsToGenerate[enumName]
		enumVals, ok := schemaMap["enum"].([]interface{})
		if !ok {
			log.Printf("Warning: Enum '%s' has invalid enum values, skipping.", enumName)
			continue
		}
		description, _ := schemaMap["description"].(string)

		// Generate type definition
		if description != "" {
			buffer.WriteString(fmt.Sprintf("// %s: %s\n", enumName, description))
		} else {
			buffer.WriteString(fmt.Sprintf("// %s defines possible values for %s.\n", enumName, enumName))
		}
		buffer.WriteString(fmt.Sprintf("type %s string\n\n", enumName))

		// Generate const block
		buffer.WriteString("const (\n")
		for _, val := range enumVals {
			strVal, ok := val.(string)
			if !ok {
				log.Printf("Warning: Enum '%s' has non-string value '%v', skipping.", enumName, val)
				continue
			}
			// Create a const name, e.g., DomainstatusAvailable from "available"
			constName := enumName + pascalCase(strVal)
			buffer.WriteString(fmt.Sprintf("\t%s %s = \"%s\"\n", constName, enumName, strVal))
		}
		buffer.WriteString(")\n\n")
	}

	// Format the generated code
	formattedCode, err := format.Source(buffer.Bytes())
	if err != nil {
		log.Printf("Warning: Failed to format common_types.go: %v", err)
		formattedCode = buffer.Bytes() // Use unformatted code on error
	}

	// Write to file
	fileName := filepath.Join(outputDir, "common_types.go")
	if err := os.WriteFile(fileName, formattedCode, 0644); err != nil {
		return "", fmt.Errorf("writing common types file %s: %w", fileName, err)
	}

	return fileName, nil
}
