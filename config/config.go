package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// EventsConfig holds general event system configuration
type EventsConfig struct {
	Kafka            KafkaConfig
	AsyncAPIURL      string
	SchemaValidation bool
	RefreshInterval  time.Duration
	KafkaTopicPrefix string
}

// DefaultEventsConfig provides default settings for the event system
func DefaultEventsConfig() EventsConfig {
	return EventsConfig{
		Kafka:            *DefaultKafkaConfig(),
		AsyncAPIURL:      GetEnv("ASYNCAPI_URL", ""),                                        // Use exported function
		SchemaValidation: GetEnvAsBool("EVENTS_SCHEMA_VALIDATION", true),                    // Use exported function
		RefreshInterval:  GetEnvAsDuration("EVENTS_SCHEMA_REFRESH_INTERVAL", 5*time.Minute), // Use exported function
		KafkaTopicPrefix: GetEnv("EVENTS_KAFKA_TOPIC_PREFIX", "nameniac.events"),            // Use exported function
	}
}

// GetEnv retrieves an environment variable or returns a fallback string.
func GetEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

// GetEnvAsBool retrieves an environment variable as a boolean or returns a fallback.
func GetEnvAsBool(key string, fallback bool) bool {
	if value, exists := os.LookupEnv(key); exists {
		// Handle common boolean string representations
		valLower := strings.ToLower(value)
		if valLower == "true" || valLower == "1" || valLower == "yes" {
			return true
		}
		if valLower == "false" || valLower == "0" || valLower == "no" {
			return false
		}
		// Try parsing with strconv as a last resort
		if b, err := strconv.ParseBool(value); err == nil {
			return b
		}
	}
	return fallback
}

// GetEnvAsDuration retrieves an environment variable as a time.Duration or returns a fallback.
func GetEnvAsDuration(key string, fallback time.Duration) time.Duration {
	if value, exists := os.LookupEnv(key); exists {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return fallback
}
