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

func main() {
	// Parse command line arguments
	asyncAPIFile := flag.String("schema", "", "Path to AsyncAPI schema file (JSON or YAML)")
	asyncAPIURL := flag.String("url", "", "URL to AsyncAPI schema")
	outputDir := flag.String("output", ".", "Directory where generated files will be stored")
	packageName := flag.String("package", "events", "Package name for generated files")
	entityTypes := flag.String("entities", "", "Comma-separated list of entity types to include (empty = all)")
	includeTypes := flag.String("include", "", "Comma-separated list of entity.action patterns to include (supports wildcards)")
	excludeTypes := flag.String("exclude", "", "Comma-separated list of entity.action patterns to exclude (supports wildcards)")
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

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Initialize generator
	generator := schema.NewGenerator(*packageName)
	if *verbose {
		generator.SetVerbose(true)
	}

	// Set filters if provided
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

	// Load schema
	loader := schema.NewSchemaLoader()
	var err error

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

	// Extract payload schemas
	payloadSchemas, err := loader.ExtractPayloadSchemas()
	if err != nil {
		log.Fatalf("Failed to extract payload schemas: %v", err)
	}

	// Extract event definitions
	eventDefs, err := loader.ExtractEventDefinitions()
	if err != nil {
		log.Fatalf("Failed to extract event definitions: %v", err)
	}

	// Generate structs
	if *verbose {
		fmt.Printf("Generating Go structs for schemas in %s\n", *outputDir)
	}

	files, err := generator.GenerateStructs(payloadSchemas, eventDefs, *outputDir)
	if err != nil {
		log.Fatalf("Failed to generate structs: %v", err)
	}

	// Print summary
	fmt.Printf("Generated %d Go files in %s:\n", len(files), *outputDir)
	for _, file := range files {
		fmt.Printf("  - %s\n", filepath.Base(file))
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
	fmt.Println("\n  # Generate only domain-related events:")
	fmt.Println("  eventgen -schema ./asyncapi.json -output ./generated -entities domain")
	fmt.Println("\n  # Generate only specific event patterns:")
	fmt.Println("  eventgen -schema ./asyncapi.json -output ./generated -include domain.analysis_*,user.created")
	fmt.Println("\n  # Generate all except certain events:")
	fmt.Println("  eventgen -schema ./asyncapi.json -output ./generated -exclude *.deleted,*.archived")
}
