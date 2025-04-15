# Stufio Go Events

A robust, type-safe event processing library for Go applications using Kafka as the message broker. Generate strongly-typed event definitions from AsyncAPI schemas, publish events, and consume them with type safety.

## Features

- 🔄 **Event Publish/Subscribe**: Simplified Kafka producer and consumer abstraction
- 🔒 **Type Safety**: Generic event handling with strong typing
- 📝 **Code Generation**: Automatic Go struct generation from AsyncAPI schemas
- ✅ **Schema Validation**: Runtime payload validation against schemas
- 🌉 **Event-Driven Architecture**: Built for microservices communication

## Installation

```bash
go get github.com/stufio-com/stufio-go-events
```

## Quick Start

### Initialize Event Client

```go
package main

import (
    "log"
    "os"
    
    goevents "github.com/stufio-com/stufio-go-events"
    eventsconfig "github.com/stufio-com/stufio-go-events/config"
)

func main() {
    // Create configuration
    kafkaCfg := eventsconfig.DefaultKafkaConfig()
    kafkaCfg.BootstrapServers = []string{"localhost:9092"}
    kafkaCfg.GroupID = "my-service"
    
    // Create client
    client, err := goevents.NewEventClient(kafkaCfg)
    if err != nil {
        log.Fatalf("Failed to create event client: %v", err)
    }
    defer client.Close()
    
    // Your application code...
}
```

## Event Types Generation

Generate Go structs from AsyncAPI schema:

1. Install the code generator:

```bash
go install github.com/stufio-com/stufio-go-events/cmd/eventgen@latest
```

2. Create a script to generate code (e.g., generate-events.sh):

```bash
#!/bin/bash

set -e  # Exit on any error

# Configuration
ASYNCAPI_FILE=${1:-"./asyncapi.json"}
OUTPUT_DIR="./generated/events"
PACKAGE_NAME="events"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Generate code
eventgen \
    -schema "$ASYNCAPI_FILE" \
    -output "$OUTPUT_DIR" \
    -package "$PACKAGE_NAME" \
    -v
```

3. Run the script:

```bash
chmod +x ./generate-events.sh
./generate-events.sh
```

This will generate:
- event_types.go: Constants for event names
- domain_events.go: Structs for each entity type
- common_types.go: Shared types like enums

## Publishing Events

### Basic Publishing

```go
// Publish an event with basic information
err := client.PublishEvent(
    "my-topic",                       // Topic name (optional, can be determined from schema)
    goevents.Entity{Type: "user", ID: "123"}, // Entity involved
    "created",                        // Action performed
    goevents.Actor{Type: "system", ID: "auth-service"}, // Actor who performed the action
    map[string]interface{}{           // Event payload
        "username": "johndoe",
        "email": "john@example.com",
    },
    "request-123",                    // Correlation ID (optional)
)
```

### Publishing with Generated Types

```go
import (
    "github.com/google/uuid"
    "github.com/myorg/myservice/generated/events"
    "github.com/stufio-com/stufio-go-events/producer"
)

// Create typed event
userCreatedEvent := &events.UserCreatedMessage{
    EventID:   uuid.New().String(),
    Timestamp: time.Now(),
    Entity: messages.Entity{
        Type: "user",
        ID:   "123",
    },
    Action: "created",
    Actor: messages.Actor{
        Type: "system",
        ID:   "auth-service",
    },
    Payload: events.UserCreatedPayload{
        Username: "johndoe",
        Email:    "john@example.com",
    },
}

// Publish to Kafka
err := producer.PublishTyped(client.producer, events.EventUserCreated, userCreatedEvent)
```

## Consuming Events

### Setup Event Handlers

```go
package main

import (
    "context"
    "log"
    "os"
    
    "github.com/myorg/myservice/generated/events"
    goevents "github.com/stufio-com/stufio-go-events"
    eventsconfig "github.com/stufio-com/stufio-go-events/config"
    "github.com/stufio-com/stufio-go-events/messages"
)

func main() {
    // Create configuration
    kafkaCfg := eventsconfig.DefaultKafkaConfig()
    kafkaCfg.BootstrapServers = []string{"localhost:9092"}
    kafkaCfg.GroupID = "analysis-service"
    // NOTE: Topics can now be configured automatically from schema
    
    // Create event config
    eventsCfg := eventsconfig.DefaultEventsConfig()
    eventsCfg.Kafka = *kafkaCfg
    eventsCfg.AsyncAPIURL = os.Getenv("ASYNCAPI_URL") // URL to AsyncAPI schema
    eventsCfg.SchemaValidation = true
    
    // Create client
    client, err := goevents.NewEventClientWithConfig(&eventsCfg)
    if err != nil {
        log.Fatalf("Failed to create event client: %v", err)
    }
    defer client.Close()
    
    // Configure topics using schema - this updates the config but doesn't initialize consumer
    client.ConfigureConsumerTopicsForActions([]string{
        "domain.analysis_requested",
    })
    
    // Initialize the consumer with the configured topics
    if err := client.EnsureConsumerInitialized(); err != nil {
        log.Fatalf("Failed to initialize consumer: %v", err)
    }
    
    // Now it's safe to register handlers
    goevents.RegisterTypedHandler(
        client,
        "domain",
        "analysis_requested",
        func(ctx context.Context, event *messages.TypedEventMessage[events.AnalysisRequestedPayload]) error {
            // Handle the event
            log.Printf("Received analysis request for domain: %s", event.Payload.Domain)
            
            // Process domain analysis...
            
            // Return nil if processing was successful
            return nil
        },
    )
    
    // Start the consumer
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    // Start in a goroutine
    go func() {
        // Check the client's topics to ensure they're configured
        if len(client.GetConsumerTopics()) > 0 {
            log.Printf("Starting event consumer for topics: %v", client.GetConsumerTopics())
            if err := client.Start(ctx); err != nil {
                log.Printf("Error starting consumer: %v", err)
                cancel()
            }
        } else {
            log.Println("No consumer topics configured, consumer not started.")
        }
    }()
    
    // Wait for termination signal
    // ... signal handling code ...
}
```

## Configuration Options

### Default Kafka Configuration

```go
// Default Kafka configuration
cfg := eventsconfig.DefaultKafkaConfig()

// Customize as needed
cfg.BootstrapServers = []string{"kafka-1:9092", "kafka-2:9092"}
cfg.GroupID = "my-service-group"
cfg.Topics = []string{"topic1", "topic2"}
cfg.AssignorStrategy = "sticky" // Options: "range", "roundrobin", "sticky"
```

### Full Events Configuration

```go
// Full events configuration
cfg := eventsconfig.DefaultEventsConfig()
cfg.Kafka = *kafkaCfg // Pass your Kafka config
cfg.AsyncAPIURL = "https://example.com/asyncapi.json" // URL to AsyncAPI schema
cfg.SchemaValidation = true // Enable schema validation
cfg.RefreshInterval = 5 * time.Minute // How often to refresh schemas
cfg.KafkaTopicPrefix = "myapp.events" // Default prefix for topics
```

## Topic Configuration

The library provides several methods to configure Kafka topics:

### Automatic Topic Configuration from AsyncAPI Schema

```go
// Configure topics for all entity types
client.ConfigureConsumerTopics([]string{"user", "domain"})

// Configure topics for specific entity-action pairs (recommended)
client.ConfigureConsumerTopicsForActions([]string{
    "user.created",
    "domain.analysis_requested",
    "domain.*"  // Wildcard for all domain actions
})

// Get currently configured topics
topics := client.GetConsumerTopics()
```

Manual vs Automatic Consumer Initialization
When creating a client, the consumer isn't automatically initialized if no topics are provided:

```go
// Create client without initializing consumer
client, err := goevents.NewEventClientWithConfig(&eventsCfg)

// Configure topics from schema
client.ConfigureConsumerTopicsForActions([]string{"user.*"})

// Manually initialize consumer after topic configuration
err := client.EnsureConsumerInitialized()
if err != nil {
    log.Fatalf("Failed to initialize consumer: %v", err)
}
```

## Code Generator Options

The `eventgen` tool accepts several flags to customize code generation:

```
eventgen -schema ./asyncapi.json -output ./generated/events -package events -entities domain,user -v
```

Available options:
- `-schema`: Path to AsyncAPI schema file (JSON)
- `-url`: URL to AsyncAPI schema
- `-output`: Directory where generated files will be stored
- `-package`: Package name for generated files
- `-entities`: Comma-separated list of entity types to include
- `-include`: Comma-separated list of entity.action patterns to include (supports wildcards)
- `-exclude`: Comma-separated list of entity.action patterns to exclude (supports wildcards)
- `-topicPrefix`: Default Kafka topic prefix
- `-v`: Verbose output
- `-help`: Show help

## Architecture

The library consists of several key components:

- **Client**: The main entry point for publishing and consuming events
- **Producer**: Handles publishing events to Kafka
- **Consumer**: Consumes events from Kafka and routes to registered handlers
- **Messages**: Core event data structures and type definitions
- **Schema**: Schema loading, validation, and code generation

## Development and Contributing

1. Clone the repository
2. Install dependencies: `go mod download`
3. Build the code generator: `./scripts/build-codegen.sh`
4. Run tests: `go test ./...`

## License

This project is licensed under the MIT License.