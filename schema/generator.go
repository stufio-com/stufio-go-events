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
// Updated signature to accept allSchemas and payloadRefs
func (g *Generator) GenerateStructs(allSchemas map[string]interface{}, payloadRefs map[string]string, messageSchemaRefs map[string]string, eventDefs []messages.EventDefinition, outputDir string) ([]string, error) {
	var files []string
	g.generatedTypes = make(map[string]bool)                    // Reset generated types tracker
	g.enumsToGenerate = make(map[string]map[string]interface{}) // Reset enums tracker

	// Filter events based on configured filters
	var filteredEventDefs []messages.EventDefinition
	for _, eventDef := range eventDefs {
		if g.shouldIncludeEvent(eventDef.EntityType, eventDef.Action) {
			filteredEventDefs = append(filteredEventDefs, eventDef)
		} else if g.verbose {
			fmt.Printf("Skipping event: %s (filtered out)\n", eventDef.Name)
		}
	}

	if len(filteredEventDefs) == 0 {
		log.Println("No events left after filtering.")
		return files, nil // No files to generate
	}

	// Generate event types file only for filtered events
	eventTypesFile, err := g.generateEventTypes(filteredEventDefs, outputDir)
	if err != nil {
		return nil, fmt.Errorf("generating event types: %w", err)
	}
	if eventTypesFile != "" {
		files = append(files, eventTypesFile)
	}

	// --- Generate entity-specific files ---
	// Group filtered events by entity type to process each entity once
	eventsByEntity := make(map[string][]messages.EventDefinition)
	for _, eventDef := range filteredEventDefs {
		eventsByEntity[eventDef.EntityType] = append(eventsByEntity[eventDef.EntityType], eventDef)
	}

	// Process each entity
	for entityType, entityEventDefs := range eventsByEntity {
		// Sort events within the entity for consistent file generation
		sort.Slice(entityEventDefs, func(i, j int) bool {
			return entityEventDefs[i].Action < entityEventDefs[j].Action
		})

		// Generate the file for this entity, passing the relevant event definitions
		// and the necessary maps for schema lookup
		fileName, err := g.generateEntityFile(entityType, entityEventDefs, allSchemas, payloadRefs, messageSchemaRefs, outputDir)
		if err != nil {
			// Log error but continue with other entities
			log.Printf("Error generating entity file for %s: %v", entityType, err)
		} else if fileName != "" {
			files = append(files, fileName)
		}
	}
	// Note: g.enumsToGenerate is populated during generateEntityFile -> generateStructForSchema -> jsonSchemaToGoType

	// --- Generate common types file AFTER processing all entities ---
	commonTypesFile, err := g.generateCommonTypesFile(outputDir)
	if err != nil {
		// Log error but potentially continue? Or make it fatal? Let's make it fatal for now.
		return files, fmt.Errorf("generating common types file: %w", err)
	}
	if commonTypesFile != "" {
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
// Updated signature to accept necessary maps
func (g *Generator) generateEntityFile(entityType string, entityEventDefs []messages.EventDefinition, allSchemas map[string]interface{}, payloadRefs map[string]string, messageSchemaRefs map[string]string, outputDir string) (string, error) {
	// Reset imports for this file
	g.imports = make(map[string]bool)

	structs := []string{}
	imports := []string{}
	payloadStructNames := make(map[string]string) // <-- DECLARE THE MAP HERE

	// --- Step 1: Generate Payload Structs --- // <-- Renamed comment for clarity
	// Iterate through the event definitions for this entity
	for _, eventDef := range entityEventDefs {
		payloadRef, refExists := payloadRefs[eventDef.Name]
		structName := pascalCase(eventDef.Action) + "Payload" // Consistent naming
		payloadStructNames[eventDef.Name] = structName        // Store the name for later use

		if !refExists {
			if g.verbose {
				log.Printf("No payload reference found for event %s, generating empty struct.", eventDef.Name)
			}
			// Generate an empty struct if no ref exists
			structName := pascalCase(eventDef.Action) + "Payload"
			structs = append(structs, fmt.Sprintf("// %s represents the payload for %s events (no schema defined)\ntype %s struct {}", structName, eventDef.Name, structName))
			continue // Move to the next event
		}

		// Parse the actual schema name from the reference
		actualSchemaName, err := parseActualSchemaName(payloadRef)
		if err != nil {
			log.Printf("Error parsing schema name from ref '%s' for event %s: %v. Skipping struct generation.", payloadRef, eventDef.Name, err)
			continue // Skip this event's struct
		}

		// Look up the schema definition in the main components map
		payloadSchemaData, schemaExists := allSchemas[actualSchemaName]
		if !schemaExists {
			log.Printf("Warning: Schema definition '%s' (from ref '%s' for event '%s') not found in components. Generating empty struct.", actualSchemaName, payloadRef, eventDef.Name)
			// Generate an empty struct if schema definition is missing
			structName := pascalCase(eventDef.Action) + "Payload"
			structs = append(structs, fmt.Sprintf("// %s represents the payload for %s events (schema '%s' not found)\ntype %s struct {}", structName, eventDef.Name, actualSchemaName, structName))
			continue // Move to the next event
		}

		// Assert that the looked-up schema is a map
		schemaMap, ok := payloadSchemaData.(map[string]interface{})
		if !ok {
			log.Printf("Warning: Schema definition '%s' for event %s is not a valid map structure. Skipping struct generation.", actualSchemaName, eventDef.Name)
			continue // Skip this event's struct
		}

		// Generate the struct definition using the found schema map
		structDef, requiredImports, err := g.generateStructForSchema(eventDef.EntityType, eventDef.Action, schemaMap)
		if err != nil {
			// Log error but continue generating other structs for this entity
			log.Printf("Error generating struct for %s: %v", eventDef.Name, err)
			continue // Skip this struct
		}

		structs = append(structs, structDef)

		// Add required imports
		for imp := range requiredImports {
			g.imports[imp] = true
		}
	}

	// --- Step 2: Generate Message Structs ---
	log.Printf("DEBUG [%s]: Starting Step 2: Generate Message Structs", entityType) // <-- Add Log
	for _, eventDef := range entityEventDefs {
		messageSchemaRef, refExists := messageSchemaRefs[eventDef.Name]
		messageStructName := pascalCase(entityType) + pascalCase(eventDef.Action) + "Message"                             // e.g., DomainAnalysisRequestedMessage
		log.Printf("DEBUG [%s]: Processing event %s for message struct %s", entityType, eventDef.Name, messageStructName) // <-- Add Log

		if !refExists {
			log.Printf("DEBUG [%s]: No message schema reference found for event %s. Skipping message struct.", entityType, eventDef.Name) // <-- Add Log
			// log.Printf("Warning: No message schema reference found for event %s. Cannot generate specific message struct %s.", eventDef.Name, messageStructName) // Original Warning
			continue // Cannot generate without the schema ref
		}
		log.Printf("DEBUG [%s]: Found message schema ref for %s: %s", entityType, eventDef.Name, messageSchemaRef) // <-- Add Log

		// Message schema ref is like "#/components/schemas/BaseEventMessage[DomainAnalysisRequestPayload]"
		// We need the schema definition itself from allSchemas
		messageSchemaKey := strings.TrimPrefix(messageSchemaRef, "#/components/schemas/")
		log.Printf("DEBUG [%s]: Looking up message schema key '%s' in allSchemas for %s", entityType, messageSchemaKey, eventDef.Name) // <-- Add Log
		messageSchemaData, schemaExists := allSchemas[messageSchemaKey]
		if !schemaExists {
			log.Printf("DEBUG [%s]: Message schema definition '%s' NOT FOUND in allSchemas for %s. Skipping message struct.", entityType, messageSchemaKey, eventDef.Name) // <-- Add Log
			// log.Printf("Warning: Message schema definition '%s' (from ref '%s' for event '%s') not found in components. Cannot generate specific message struct %s.", messageSchemaKey, messageSchemaRef, eventDef.Name, messageStructName) // Original Warning
			continue
		}
		log.Printf("DEBUG [%s]: Found message schema definition for key '%s'", entityType, messageSchemaKey) // <-- Add Log

		messageSchemaMap, ok := messageSchemaData.(map[string]interface{})
		if !ok {
			log.Printf("DEBUG [%s]: Message schema definition '%s' for event %s is not a valid map. Skipping message struct.", entityType, messageSchemaKey, eventDef.Name) // <-- Add Log
			// log.Printf("Warning: Message schema definition '%s' for event %s is not a valid map structure. Cannot generate specific message struct %s.", messageSchemaKey, eventDef.Name, messageStructName) // Original Warning
			continue
		}
		log.Printf("DEBUG [%s]: Message schema definition for key '%s' is a valid map.", entityType, messageSchemaKey) // <-- Add Log

		// Get the name of the payload struct we generated in Step 1
		payloadStructName := payloadStructNames[eventDef.Name]
		if payloadStructName == "" {
			log.Printf("DEBUG [%s]: Could not find payload struct name for event %s in payloadStructNames map. Skipping message struct.", entityType, eventDef.Name) // <-- Add Log
			// log.Printf("Warning: Could not find payload struct name for event %s. Cannot generate specific message struct %s.", eventDef.Name, messageStructName) // Original Warning
			continue // Should not happen if Step 1 worked
		}
		log.Printf("DEBUG [%s]: Found corresponding payload struct name: %s", entityType, payloadStructName) // <-- Add Log

		// Generate the message struct definition
		log.Printf("DEBUG [%s]: Calling generateMessageStructForSchema for %s (Payload: %s)", entityType, messageStructName, payloadStructName) // <-- Add Log
		structDef, requiredImports, err := g.generateMessageStructForSchema(messageStructName, payloadStructName, messageSchemaMap)
		if err != nil {
			log.Printf("DEBUG [%s]: Error from generateMessageStructForSchema for %s: %v. Skipping.", entityType, messageStructName, err) // <-- Add Log
			// log.Printf("Error generating message struct %s for %s: %v", messageStructName, eventDef.Name, err) // Original Error
			continue
		}
		log.Printf("DEBUG [%s]: Successfully generated message struct definition for %s.", entityType, messageStructName) // <-- Add Log
		structs = append(structs, structDef)
		for imp := range requiredImports {
			g.imports[imp] = true
		}
	}
	log.Printf("DEBUG [%s]: Finished Step 2: Generate Message Structs", entityType) // <-- Add Log

	// If no structs were successfully generated for this entity, don't create the file
	if len(structs) == 0 {
		if g.verbose {
			log.Printf("No structs generated for entity file: %s", entityType)
		}
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
		// "StructName": pascalCase(entityType), // Not needed here
		"Structs": structs,
		"Imports": imports,
		// "Events":     entityEventDefs, // Keep events for potential future use in template
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
		// Resolve the ref to the Go type name using the updated function
		typeName, err := g.resolveRefToTypeName(ref)
		if err != nil {
			log.Printf("Warning: Failed to resolve ref '%s': %v. Using 'interface{}'.", ref, err)
			return "interface{}", imports // Fallback to interface{} on error
		}

		// Check if the resolved type is an enum we need to generate
		if _, isEnum := g.enumsToGenerate[typeName]; isEnum {
			// It's an enum, return the type name (it will be generated later)
			return typeName, imports
		}

		// Check if the resolved type is a known primitive or special type (like time.Time)
		// This requires looking up the actual schema definition for the ref.
		refSchema, err := g.lookupComponentSchema(ref) // Use existing lookup
		if err != nil {
			log.Printf("Warning: Could not look up schema for ref '%s': %v. Assuming object type.", ref, err)
			// Assume it's a struct to be generated if lookup fails
			g.generatedTypes[typeName] = true // Mark for potential generation
			return typeName, imports
		}

		// Determine the type of the referenced schema
		refSchemaType, _ := refSchema["type"].(string)
		refSchemaFormat, _ := refSchema["format"].(string)

		if refSchemaType == "string" && refSchemaFormat == "date-time" {
			imports["time"] = true
			return "time.Time", imports
		}
		if refSchemaType == "string" && len(refSchema["enum"].([]interface{})) > 0 {
			// It's an enum defined by ref, ensure it's marked for generation
			g.enumsToGenerate[typeName] = refSchema
			return typeName, imports
		}
		if refSchemaType != "object" && refSchemaType != "" { // If it's a primitive type by reference
			primitiveType, primitiveImports := g.jsonSchemaToGoType(refSchema) // Recurse for primitives/arrays
			for imp := range primitiveImports {
				imports[imp] = true
			}
			return primitiveType, imports
		}

		// Otherwise, assume it's an object/struct type
		g.generatedTypes[typeName] = true // Mark for generation
		return typeName, imports
	}

	// Handle combined types like anyOf, oneOf (use interface{} for simplicity for now)
	if _, exists := schema["anyOf"]; exists {
		log.Println("Warning: anyOf type found, using interface{}") // Warning source
		return "interface{}", nil                                   // Defaults to interface{}
	}
	if _, exists := schema["oneOf"]; exists {
		log.Println("Warning: oneOf type found, using interface{}") // Similar handling
		return "interface{}", nil
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
	// Use the same parser as in asyncapi.go
	actualSchemaName, err := parseActualSchemaName(ref) // Assuming parseActualSchemaName is accessible or duplicated here
	if err != nil {
		return "", fmt.Errorf("parsing ref '%s': %w", ref, err)
	}

	// Convert schema name (e.g., UserCreatedPayload) to Go type name (e.g., UserCreatedPayload)
	typeName := pascalCase(actualSchemaName) // Use existing helper

	// Check if the referenced type itself needs generation (if it's an object)
	// This logic might be better placed within jsonSchemaToGoType when handling the $ref
	// For now, just return the PascalCase name.
	return typeName, nil
}

// --- Add or ensure parseActualSchemaName is available ---
// Parses "BaseEventMessage[ActualName]" or "ActualName" from a ref like "#/components/schemas/..."
// Removed duplicate parseActualSchemaName function to avoid redeclaration error.

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

// generateMessageStructForSchema generates a Go struct definition for a full message schema
// Ensure it starts with func (g *Generator) ...
func (g *Generator) generateMessageStructForSchema(
	structName string, // e.g., DomainAnalysisRequestedMessage
	payloadStructName string, // e.g., DomainAnalysisRequestedPayload
	schemaMap map[string]interface{},
) (string, map[string]bool, error) {

	var buffer bytes.Buffer
	requiredImports := make(map[string]bool)

	// Add description from schema if available
	if description, ok := schemaMap["description"].(string); ok && description != "" {
		buffer.WriteString(fmt.Sprintf("// %s: %s\n", structName, description))
	} else if title, ok := schemaMap["title"].(string); ok && title != "" {
		buffer.WriteString(fmt.Sprintf("// %s: Based on schema '%s'\n", structName, title))
	} else {
		buffer.WriteString(fmt.Sprintf("// %s represents the structure for a specific event message.\n", structName))
	}
	buffer.WriteString(fmt.Sprintf("type %s struct {\n", structName))

	// Get required fields from the message schema
	requiredFields := make(map[string]bool)
	if reqList, ok := schemaMap["required"].([]interface{}); ok {
		for _, req := range reqList {
			if reqStr, ok := req.(string); ok {
				requiredFields[reqStr] = true
			}
		}
	}

	// Define standard BaseEventMessage fields and their JSON tags
	// We assume the message schema follows the BaseEventMessage structure
	fields := map[string]string{
		"EventID":       "string",
		"CorrelationID": "*string", // Pointer for omitempty
		"Timestamp":     "time.Time",
		"Entity":        "messages.Entity",
		"Action":        "string",
		"Actor":         "messages.Actor",
		"Payload":       payloadStructName,        // Use the specific payload type name
		"Metrics":       "*messages.EventMetrics", // Pointer for omitempty
	}
	jsonTags := map[string]string{
		"EventID":       "event_id",
		"CorrelationID": "correlation_id",
		"Timestamp":     "timestamp",
		"Entity":        "entity",
		"Action":        "action",
		"Actor":         "actor",
		"Payload":       "payload",
		"Metrics":       "metrics",
	}

	// Order fields for consistent output
	fieldOrder := []string{"EventID", "CorrelationID", "Timestamp", "Entity", "Action", "Actor", "Payload", "Metrics"}

	for _, fieldName := range fieldOrder {
		fieldType := fields[fieldName]
		jsonTag := jsonTags[fieldName]

		// Add imports based on type
		if fieldType == "time.Time" {
			requiredImports["time"] = true
		}
		if strings.HasPrefix(fieldType, "messages.") || strings.HasPrefix(fieldType, "*messages.") {
			// TODO: Make this import path configurable or determine it more reliably.
			// Assuming the generated code is in a subpackage of the project using the library.
			// If the generator is part of the library itself, this might need adjustment.
			// For now, assume the library is imported as github.com/stufio-com/stufio-go-events
			requiredImports["github.com/stufio-com/stufio-go-events/messages"] = true
		}

		// Determine omitempty
		omitempty := ",omitempty"
		if requiredFields[jsonTag] {
			omitempty = "" // Remove omitempty if the field is required by the message schema
		} else if fieldName != "CorrelationID" && fieldName != "Metrics" {
			// By default, only make CorrelationID and Metrics omitempty unless required.
			// All other base fields are assumed mandatory for a valid event message structure.
			// If the schema explicitly requires CorrelationID or Metrics, omitempty will be removed above.
		}

		buffer.WriteString(fmt.Sprintf("\t%s %s `json:\"%s%s\"`\n", fieldName, fieldType, jsonTag, omitempty))
	}

	buffer.WriteString("}\n\n")

	return buffer.String(), requiredImports, nil
}
