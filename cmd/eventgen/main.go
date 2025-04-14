package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/stufio-com/stufio-go-events/schema"
)

const (
	// DefaultKafkaTopicPrefix is the default prefix if none is provided
	DefaultKafkaTopicPrefix = "nameniac.events"
	// KafkaTopicPrefixEnvVar is the environment variable to check for the prefix
	KafkaTopicPrefixEnvVar = "EVENTS_KAFKA_TOPIC_PREFIX"
)

func main() {
	// Parse command line arguments
	asyncAPIFile := flag.String("schema", "", "Path to AsyncAPI schema file (JSON or YAML)")
	asyncAPIURL := flag.String("url", "", "URL to AsyncAPI schema")
	outputDir := flag.String("output", ".", "Directory where generated files will be stored")
	packageName := flag.String("package", "events", "Package name for generated files")
	entityTypes := flag.String("entities", "", "Comma-separated list of entity types to include (empty = all)")
	includeTypes := flag.String("include", "", "Comma-separated list of entity.action patterns to include (supports wildcards)")
	excludeTypes := flag.String("exclude", "", "Comma-separated list of entity.action patterns to exclude (supports wildcards)")
	topicPrefixFlag := flag.String("topicPrefix", "", fmt.Sprintf("Default Kafka topic prefix (overrides %s env var)", KafkaTopicPrefixEnvVar)) // <-- Add flag
	verbose := flag.Bool("v", false, "Verbose output")
	help := flag.Bool("help", false, "Show help")

	flag.Parse()

	if *help {
		printUsage()
		return
	}

	// Validate arguments
	if *asyncAPIFile == "" && *asyncAPIURL == "" {
		fmt.Println("Error: Either a schema file path or URL must be provided")
		printUsage()
		os.Exit(1)
	}

	// --- Determine Topic Prefix ---
	topicPrefix := DefaultKafkaTopicPrefix // Start with default
	envPrefix := os.Getenv(KafkaTopicPrefixEnvVar)
	if envPrefix != "" {
		topicPrefix = envPrefix // Use environment variable if set
	}
	if *topicPrefixFlag != "" {
		topicPrefix = *topicPrefixFlag // Use flag if provided (highest priority)
	}
	if *verbose {
		fmt.Printf("Using Kafka topic prefix: %s\n", topicPrefix)
	}
	// --- End Determine Topic Prefix ---

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Load schema
	loader := schema.NewSchemaLoader()
	var err error
	// var loadedSchema *schema.AsyncAPISchema // Store the loaded schema - Not strictly needed here

	if *asyncAPIFile != "" {
		if *verbose {
			fmt.Printf("Loading AsyncAPI schema from file: %s\n", *asyncAPIFile)
		}
		err = loader.LoadFromFile(*asyncAPIFile)
	} else {
		if *verbose {
			fmt.Printf("Loading AsyncAPI schema from URL: %s\n", *asyncAPIURL)
		}
		err = loader.LoadFromURL(*asyncAPIURL)
	}

	if err != nil {
		log.Fatalf("Failed to load AsyncAPI schema: %v", err)
	}

	// Get the loaded schema's components
	schemaComponents, ok := loader.GetSchemaComponents()
	if !ok {
		log.Fatalf("Failed to get schema components after loading")
	}

	// Initialize generator *after* loading schema and getting components
	generator := schema.NewGenerator(*packageName, schemaComponents.Schemas) // Pass components.Schemas

	// Set verbosity and filters *after* initializing the generator
	if *verbose {
		generator.SetVerbose(true)
	}

	if *entityTypes != "" {
		entities := strings.Split(*entityTypes, ",")
		for i, e := range entities {
			entities[i] = strings.TrimSpace(e)
		}
		generator.FilterByEntityTypes(entities)
	}

	if *includeTypes != "" {
		patterns := strings.Split(*includeTypes, ",")
		for i, p := range patterns {
			patterns[i] = strings.TrimSpace(p)
		}
		generator.SetIncludePatterns(patterns)
	}

	if *excludeTypes != "" {
		patterns := strings.Split(*excludeTypes, ",")
		for i, p := range patterns {
			patterns[i] = strings.TrimSpace(p)
		}
		generator.SetExcludePatterns(patterns)
	}

	// Extract payload schemas (now returns map[string]map[string]interface{})
	// We primarily need the payloadRefs map from this.
	_, payloadRefs, err := loader.ExtractPayloadSchemasAndRefs() // Keep payloadRefs
	if err != nil {
		// Log non-fatal errors from ExtractPayloadSchemas already, just check final error state if needed
		log.Printf("Warning: Encountered errors during payload schema extraction: %v", err)
		// Decide if this should be fatal or not. Let's allow continuing.
	}
	// We don't strictly need to fail if payloadRefs is empty,
	// as some events might genuinely have no payload.
	// The generator should handle missing refs gracefully.

	// Extract event definitions - Pass the determined topic prefix
	eventDefs, err := loader.ExtractEventDefinitions(topicPrefix) // <-- Pass determined prefix
	if err != nil {
		log.Fatalf("Failed to extract event definitions: %v", err)
	}
	if len(eventDefs) == 0 {
		log.Fatalf("No event definitions were successfully extracted. Check warnings.")
	}

	// Generate structs
	if *verbose {
		// Correct the log message if needed, based on what GenerateStructs uses
		fmt.Printf("Generating Go structs based on %d event definitions in %s\n", len(eventDefs), *outputDir)
	}

	// Pass the full component schemas and the map of payload references
	files, err := generator.GenerateStructs(schemaComponents.Schemas, payloadRefs, eventDefs, *outputDir)
	if err != nil {
		log.Fatalf("Failed to generate structs: %v", err) // This captures errors from file writing etc.
	}

	// Print summary
	if len(files) > 0 {
		fmt.Printf("Generated %d Go files in %s:\n", len(files), *outputDir)
		for _, file := range files {
			fmt.Printf("  - %s\n", filepath.Base(file))
		}
	} else {
		fmt.Println("No Go files were generated. Check logs for warnings or errors.")
	}
}

func printUsage() {
	fmt.Println("Event Code Generator for AsyncAPI Schemas")
	fmt.Println("\nUsage:")
	fmt.Println("  eventgen [options]")
	fmt.Println("\nOptions:")
	flag.PrintDefaults()
	fmt.Println("\nExamples:")
	fmt.Println("  # Generate all events:")
	fmt.Println("  eventgen -schema ./asyncapi.json -output ./generated -package myevents")
	fmt.Println("\n  # Generate with a specific topic prefix:")
	fmt.Println("  eventgen -schema ./asyncapi.json -output ./generated -topicPrefix myapp.events")
	fmt.Println("\n  # Generate using environment variable for prefix:")
	fmt.Println("  export EVENTS_KAFKA_TOPIC_PREFIX=myapp.events && eventgen -schema ./asyncapi.json -output ./generated")
	fmt.Println("\n  # Generate only domain-related events:")
	fmt.Println("  eventgen -schema ./asyncapi.json -output ./generated -entities domain")
	fmt.Println("\n  # Generate only specific event patterns:")
	fmt.Println("  eventgen -schema ./asyncapi.json -output ./generated -include domain.analysis_*,user.created")
	fmt.Println("\n  # Generate all except certain events:")
	fmt.Println("  eventgen -schema ./asyncapi.json -output ./generated -exclude *.deleted,*.archived")
}
