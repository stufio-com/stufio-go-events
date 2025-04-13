package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// KafkaConfig holds configuration for Kafka connections
type KafkaConfig struct {
	BootstrapServers  string
	GroupID           string
	Topics            []string
	AutoOffsetReset   string
	AssignorStrategy  string
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	MaxPollInterval   time.Duration
	SecurityProtocol  string
	SASLMechanism     string
	SASLUsername      string
	SASLPassword      string
}

// DefaultKafkaConfig returns a default configuration
func DefaultKafkaConfig() *KafkaConfig {
	return &KafkaConfig{
		BootstrapServers:  "localhost:9092",
		GroupID:           "stufio-go-events",
		Topics:            []string{},
		AutoOffsetReset:   "earliest",
		AssignorStrategy:  "roundrobin",
		SessionTimeout:    30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		MaxPollInterval:   5 * time.Minute,
		SecurityProtocol:  "plaintext",
		SASLMechanism:     "",
		SASLUsername:      "",
		SASLPassword:      "",
	}
}

// NewKafkaConfigFromEnv creates a KafkaConfig from environment variables
func NewKafkaConfigFromEnv() *KafkaConfig {
	config := DefaultKafkaConfig()

	if servers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS"); servers != "" {
		config.BootstrapServers = servers
	}

	if groupID := os.Getenv("KAFKA_GROUP_ID"); groupID != "" {
		config.GroupID = groupID
	}

	if topics := os.Getenv("KAFKA_TOPICS"); topics != "" {
		config.Topics = strings.Split(topics, ",")
	}

	if reset := os.Getenv("KAFKA_AUTO_OFFSET_RESET"); reset != "" {
		config.AutoOffsetReset = reset
	}

	if assignor := os.Getenv("KAFKA_ASSIGNOR_STRATEGY"); assignor != "" {
		config.AssignorStrategy = assignor
	}

	if timeout := os.Getenv("KAFKA_SESSION_TIMEOUT_MS"); timeout != "" {
		if ms, err := time.ParseDuration(timeout + "ms"); err == nil {
			config.SessionTimeout = ms
		}
	}

	if interval := os.Getenv("KAFKA_HEARTBEAT_INTERVAL_MS"); interval != "" {
		if ms, err := time.ParseDuration(interval + "ms"); err == nil {
			config.HeartbeatInterval = ms
		}
	}

	if pollInterval := os.Getenv("KAFKA_MAX_POLL_INTERVAL_MS"); pollInterval != "" {
		if ms, err := time.ParseDuration(pollInterval + "ms"); err == nil {
			config.MaxPollInterval = ms
		}
	}

	if protocol := os.Getenv("KAFKA_SECURITY_PROTOCOL"); protocol != "" {
		config.SecurityProtocol = protocol
	}

	if mechanism := os.Getenv("KAFKA_SASL_MECHANISM"); mechanism != "" {
		config.SASLMechanism = mechanism
	}

	if username := os.Getenv("KAFKA_SASL_USERNAME"); username != "" {
		config.SASLUsername = username
	}

	if password := os.Getenv("KAFKA_SASL_PASSWORD"); password != "" {
		config.SASLPassword = password
	}

	return config
}

// CreateConsumerConfig creates a Sarama ConsumerGroup configuration
func (c *KafkaConfig) CreateConsumerConfig() (*sarama.Config, error) {
	config := sarama.NewConfig()

	// Version configuration
	version, err := sarama.ParseKafkaVersion("2.8.0") // Recent stable version
	if err != nil {
		return nil, fmt.Errorf("parsing Kafka version: %w", err)
	}
	config.Version = version

	// Consumer group settings
	switch c.AssignorStrategy {
	case "sticky":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	case "roundrobin":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	case "range":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	default:
		return nil, fmt.Errorf("unrecognized consumer group partition assignor: %s", c.AssignorStrategy)
	}

	// Offset management
	if c.AutoOffsetReset == "earliest" {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else if c.AutoOffsetReset == "latest" {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	// Timeouts and intervals
	config.Consumer.Group.Session.Timeout = c.SessionTimeout
	config.Consumer.Group.Heartbeat.Interval = c.HeartbeatInterval
	config.Consumer.MaxProcessingTime = c.MaxPollInterval

	// Auto-commit settings
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 5 * time.Second

	// Configure SASL if needed
	if c.SecurityProtocol == "sasl_plaintext" || c.SecurityProtocol == "sasl_ssl" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = c.SASLUsername
		config.Net.SASL.Password = c.SASLPassword

		switch c.SASLMechanism {
		case "plain":
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "scram-sha-256":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
			}
		case "scram-sha-512":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
			}
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", c.SASLMechanism)
		}
	}

	// SSL configuration if needed
	if c.SecurityProtocol == "ssl" || c.SecurityProtocol == "sasl_ssl" {
		config.Net.TLS.Enable = true
	}

	return config, nil
}

// CreateProducerConfig creates a Sarama Producer configuration
func (c *KafkaConfig) CreateProducerConfig() (*sarama.Config, error) {
	config := sarama.NewConfig()

	// Version configuration
	version, err := sarama.ParseKafkaVersion("2.8.0")
	if err != nil {
		return nil, fmt.Errorf("parsing Kafka version: %w", err)
	}
	config.Version = version

	// Producer configuration
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	// Configure SASL if needed
	if c.SecurityProtocol == "sasl_plaintext" || c.SecurityProtocol == "sasl_ssl" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = c.SASLUsername
		config.Net.SASL.Password = c.SASLPassword

		switch c.SASLMechanism {
		case "plain":
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "scram-sha-256":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "scram-sha-512":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", c.SASLMechanism)
		}
	}

	// SSL configuration if needed
	if c.SecurityProtocol == "ssl" || c.SecurityProtocol == "sasl_ssl" {
		config.Net.TLS.Enable = true
	}

	return config, nil
}

// GetBrokersList returns slice of brokers from the bootstrap servers string
func (c *KafkaConfig) GetBrokersList() []string {
	return strings.Split(c.BootstrapServers, ",")
}

// SASL authentication utility structures - placeholders for now
// Would need actual implementation for SCRAM authentication
type XDGSCRAMClient struct {
	HashGeneratorFcn func() sarama.SCRAMClient
}

func SHA256() sarama.SCRAMClient {
	// Placeholder
	return nil
}

func SHA512() sarama.SCRAMClient {
	// Placeholder
	return nil
}
