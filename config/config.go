package config

import (
	"os"
	"strconv"
	"time"
)

// EventsConfig holds global configuration for the events system
type EventsConfig struct {
	// Kafka configuration
	Kafka *KafkaConfig

	// Schema configuration
	AsyncAPIURL       string
	SchemaValidation  bool
	SchemaRefreshTime time.Duration
}

// DefaultEventsConfig returns a default configuration
func DefaultEventsConfig() *EventsConfig {
	return &EventsConfig{
		Kafka:             DefaultKafkaConfig(),
		AsyncAPIURL:       "http://localhost:8000/asyncapi.json",
		SchemaValidation:  true,
		SchemaRefreshTime: 15 * time.Minute,
	}
}

// NewEventsConfigFromEnv creates a config from environment variables
func NewEventsConfigFromEnv() *EventsConfig {
	config := DefaultEventsConfig()

	if url := os.Getenv("ASYNCAPI_URL"); url != "" {
		config.AsyncAPIURL = url
	}

	if validation := os.Getenv("SCHEMA_VALIDATION"); validation != "" {
		if b, err := strconv.ParseBool(validation); err == nil {
			config.SchemaValidation = b
		}
	}

	if refresh := os.Getenv("SCHEMA_REFRESH_TIME"); refresh != "" {
		if d, err := time.ParseDuration(refresh); err == nil {
			config.SchemaRefreshTime = d
		}
	}

	return config
}
