package schema

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/stufio-com/stufio-go-events/config" // Import config if needed for topic prefix
	"github.com/stufio-com/stufio-go-events/messages"
	"github.com/xeipuuv/gojsonschema"
)

// --- Add extractKafkaTopic function ---
// extractKafkaTopic extracts the Kafka topic name from channel bindings
func extractKafkaTopic(channel Channel) string {
	if kafkaBinding, ok := channel.Bindings["kafka"].(map[string]interface{}); ok {
		if topic, ok := kafkaBinding["topic"].(string); ok && topic != "" {
			return topic
		}
	}
	return "" // Return empty if not found
}

// --- Add parseActualSchemaName function ---
// parseActualSchemaName extracts the core schema name from a reference string,
// handling patterns like "#/components/schemas/BaseEventMessage[ActualName]"
// or "#/components/schemas/ActualName".
func parseActualSchemaName(ref string) (string, error) {
	if !strings.HasPrefix(ref, "#/components/schemas/") {
		return "", fmt.Errorf("invalid schema reference format: %s", ref)
	}
	baseName := strings.TrimPrefix(ref, "#/components/schemas/")

	// Check for the BaseEventMessage[ActualName] pattern
	if strings.Contains(baseName, "BaseEventMessage[") && strings.HasSuffix(baseName, "]") {
		start := strings.Index(baseName, "[") + 1
		end := strings.LastIndex(baseName, "]")
		if start > 0 && end > start {
			return baseName[start:end], nil // Return "ActualName"
		}
		return "", fmt.Errorf("failed to extract name from BaseEventMessage pattern: %s", baseName)
	}

	// Otherwise, return the base name directly
	return baseName, nil
}

// ExtractEventDefinitions extracts event definitions from the schema
// Updated to accept topicPrefix
func (l *SchemaLoader) ExtractEventDefinitions(topicPrefix string) ([]messages.EventDefinition, map[string]string, error) {
	if l.schema == nil {
		return nil, nil, fmt.Errorf("no schema loaded")
	}

	var definitions []messages.EventDefinition
	messageSchemaRefs := make(map[string]string) // <-- Initialize the correct map

	// Process each channel
	for channelName, channel := range l.schema.Channels {
		// Find Kafka topic from bindings
		topic := extractKafkaTopic(channel) // Use the new function
		if topic == "" {
			// --- Use prefix for default topic ---
			// Use the provided topicPrefix if no binding found
			topic = topicPrefix
		}

		// Process publish operation (events we can consume)
		if channel.Publish != nil && channel.Publish.Message != nil { // Check Message is not nil
			var resolvedMsg *Message                                             // Declare resolvedMsg as *Message
			resolvedMsgName, err := l.resolveMessageRef(channel.Publish.Message) // Get name or ""
			if err != nil {
				log.Printf("Warning: Skipping channel '%s' publish op due to message resolve error: %v", channelName, err)
				continue // Skip this operation
			}

			if resolvedMsgName != "" { // It's a reference, look it up
				if l.schema.Components.Messages == nil {
					log.Printf("Warning: Skipping channel '%s' publish op: Components.Messages is nil, cannot lookup resolved message '%s'", channelName, resolvedMsgName)
					continue
				}
				msgCopy, found := l.schema.Components.Messages[resolvedMsgName]
				if !found {
					log.Printf("Warning: Skipping channel '%s' publish op: resolved message '%s' not found in map", channelName, resolvedMsgName)
					continue
				}
				resolvedMsg = &msgCopy // Assign pointer to the copy
			} else { // It's an inline message
				resolvedMsg = channel.Publish.Message // Use the inline message directly
			}

			// Pass prefix and the resolved *Message to extractEventFromOperation
			event, err := extractEventFromOperation(channelName, topic, channel.Publish, resolvedMsg, channel.Description, topicPrefix) // Pass resolvedMsg (*Message)
			if err != nil {
				// Log error but continue processing other channels/operations
				log.Printf("Warning: Error extracting publish event from channel %s: %v", channelName, err)
				continue
			}
			definitions = append(definitions, event)

			// --- Store the message schema ref ---
			log.Printf("DEBUG [ExtractEventDefinitions]: Processing event '%s' for message schema ref extraction.", event.Name) // <-- Add Log
			if resolvedMsg != nil && resolvedMsg.Payload != nil {
				log.Printf("DEBUG [ExtractEventDefinitions]: Event '%s' has resolvedMsg and Payload map: %+v", event.Name, resolvedMsg.Payload) // <-- Add Log

				// Find the $ref within the resolved message's payload field
				// This ref points to the schema defining the BaseEventMessage structure
				if refVal, ok := resolvedMsg.Payload["$ref"]; ok { // Check for $ref key in payload map
					log.Printf("DEBUG [ExtractEventDefinitions]: Event '%s' payload has '$ref' key.", event.Name) // <-- Add Log
					if refStr, ok := refVal.(string); ok {
						log.Printf("DEBUG [ExtractEventDefinitions]: Found message schema ref '%s' for event '%s'", refStr, event.Name) // <-- Add Log
						messageSchemaRefs[event.Name] = refStr                                                                          // event.Name is entity.action
					} else {
						log.Printf("DEBUG [ExtractEventDefinitions]: Payload $ref value is NOT a string for event '%s': Type=%T, Value=%+v", event.Name, refVal, refVal) // <-- Add Log
					}
				} else if oneOfVal, ok := resolvedMsg.Payload["oneOf"]; ok { // Check for oneOf key
					log.Printf("DEBUG [ExtractEventDefinitions]: Event '%s' payload has 'oneOf' key.", event.Name) // <-- Add Log
					// Handle oneOf case if necessary, find the $ref inside
					if oneOfList, ok := oneOfVal.([]interface{}); ok && len(oneOfList) > 0 {
						log.Printf("DEBUG [ExtractEventDefinitions]: Event '%s' payload 'oneOf' is a list with %d items.", event.Name, len(oneOfList)) // <-- Add Log
						if firstChoice, ok := oneOfList[0].(map[string]interface{}); ok {
							log.Printf("DEBUG [ExtractEventDefinitions]: Event '%s' payload 'oneOf'[0] is a map: %+v", event.Name, firstChoice) // <-- Add Log
							if refVal, ok := firstChoice["$ref"]; ok {
								log.Printf("DEBUG [ExtractEventDefinitions]: Event '%s' payload 'oneOf'[0] has '$ref' key.", event.Name) // <-- Add Log
								if refStr, ok := refVal.(string); ok {
									log.Printf("DEBUG [ExtractEventDefinitions]: Found message schema ref '%s' (from oneOf) for event '%s'", refStr, event.Name) // <-- Add Log
									messageSchemaRefs[event.Name] = refStr
								} else {
									log.Printf("DEBUG [ExtractEventDefinitions]: Payload oneOf $ref value is NOT a string for event '%s': Type=%T, Value=%+v", event.Name, refVal, refVal) // <-- Add Log
								}
							} else {
								log.Printf("DEBUG [ExtractEventDefinitions]: Payload oneOf item has NO $ref for event '%s': %+v", event.Name, firstChoice) // <-- Add Log
							}
						} else {
							log.Printf("DEBUG [ExtractEventDefinitions]: Payload oneOf item is NOT a map for event '%s': Type=%T, Value=%+v", event.Name, oneOfList[0], oneOfList[0]) // <-- Add Log
						}
					} else {
						log.Printf("DEBUG [ExtractEventDefinitions]: Payload oneOf value is NOT a list or is empty for event '%s': Type=%T, Value=%+v", event.Name, oneOfVal, oneOfVal) // <-- Add Log
					}
				} else {
					log.Printf("DEBUG [ExtractEventDefinitions]: Payload for event '%s' has NEITHER $ref NOR oneOf key: %+v", event.Name, resolvedMsg.Payload) // <-- Add Log
				}
			} else {
				errMsg := "resolvedMsg is nil"
				if resolvedMsg != nil { // Implies Payload is nil
					errMsg = "resolvedMsg.Payload is nil"
				}
				log.Printf("DEBUG [ExtractEventDefinitions]: Skipping message schema ref extraction for event '%s' (%s)", event.Name, errMsg) // <-- Add Log
			}
			// --- End store message schema ref ---
		}

		// Process subscribe operation (events we can produce)
		if channel.Subscribe != nil && channel.Subscribe.Message != nil { // Check Message is not nil
			var resolvedMsg *Message                                               // Declare resolvedMsg as *Message
			resolvedMsgName, err := l.resolveMessageRef(channel.Subscribe.Message) // Get name or ""
			if err != nil {
				log.Printf("Warning: Skipping channel '%s' subscribe op due to message resolve error: %v", channelName, err)
				continue // Skip this operation
			}

			if resolvedMsgName != "" { // It's a reference, look it up
				if l.schema.Components.Messages == nil {
					log.Printf("Warning: Skipping channel '%s' subscribe op: Components.Messages is nil, cannot lookup resolved message '%s'", channelName, resolvedMsgName)
					continue
				}
				msgCopy, found := l.schema.Components.Messages[resolvedMsgName]
				if !found {
					log.Printf("Warning: Skipping channel '%s' subscribe op: resolved message '%s' not found in map", channelName, resolvedMsgName)
					continue
				}
				resolvedMsg = &msgCopy // Assign pointer to the copy
			} else { // It's an inline message
				resolvedMsg = channel.Subscribe.Message // Use the inline message directly
			}

			// Pass prefix and the resolved *Message to extractEventFromOperation
			event, err := extractEventFromOperation(channelName, topic, channel.Subscribe, resolvedMsg, channel.Description, topicPrefix) // Pass resolvedMsg (*Message)
			if err != nil {
				// Log error but continue processing other channels/operations
				log.Printf("Warning: Error extracting subscribe event from channel %s: %v", channelName, err)
				continue
			}
			definitions = append(definitions, event)

			// --- Store the message schema ref ---
			log.Printf("DEBUG [ExtractEventDefinitions]: Processing event '%s' for message schema ref extraction.", event.Name) // <-- Add Log
			if resolvedMsg != nil && resolvedMsg.Payload != nil {
				log.Printf("DEBUG [ExtractEventDefinitions]: Event '%s' has resolvedMsg and Payload map: %+v", event.Name, resolvedMsg.Payload) // <-- Add Log

				// Find the $ref within the resolved message's payload field
				// This ref points to the schema defining the BaseEventMessage structure
				if refVal, ok := resolvedMsg.Payload["$ref"]; ok { // Check for $ref key in payload map
					log.Printf("DEBUG [ExtractEventDefinitions]: Event '%s' payload has '$ref' key.", event.Name) // <-- Add Log
					if refStr, ok := refVal.(string); ok {
						log.Printf("DEBUG [ExtractEventDefinitions]: Found message schema ref '%s' for event '%s'", refStr, event.Name) // <-- Add Log
						messageSchemaRefs[event.Name] = refStr                                                                          // Store the message schema ref
					} else {
						log.Printf("DEBUG [ExtractEventDefinitions]: Payload $ref value is NOT a string for event '%s': Type=%T, Value=%+v", event.Name, refVal, refVal) // <-- Add Log
					}
				} else if oneOfVal, ok := resolvedMsg.Payload["oneOf"]; ok { // Check for oneOf key
					log.Printf("DEBUG [ExtractEventDefinitions]: Event '%s' payload has 'oneOf' key.", event.Name) // <-- Add Log
					// Handle oneOf case if necessary, find the $ref inside
					if oneOfList, ok := oneOfVal.([]interface{}); ok && len(oneOfList) > 0 {
						log.Printf("DEBUG [ExtractEventDefinitions]: Event '%s' payload 'oneOf' is a list with %d items.", event.Name, len(oneOfList)) // <-- Add Log
						if firstChoice, ok := oneOfList[0].(map[string]interface{}); ok {
							log.Printf("DEBUG [ExtractEventDefinitions]: Event '%s' payload 'oneOf'[0] is a map: %+v", event.Name, firstChoice) // <-- Add Log
							if refVal, ok := firstChoice["$ref"]; ok {
								log.Printf("DEBUG [ExtractEventDefinitions]: Event '%s' payload 'oneOf'[0] has '$ref' key.", event.Name) // <-- Add Log
								if refStr, ok := refVal.(string); ok {
									log.Printf("DEBUG [ExtractEventDefinitions]: Found message schema ref '%s' (from oneOf) for event '%s'", refStr, event.Name) // <-- Add Log
									messageSchemaRefs[event.Name] = refStr                                                                                       // Store the message schema ref
								} else {
									log.Printf("DEBUG [ExtractEventDefinitions]: Payload oneOf $ref value is NOT a string for event '%s': Type=%T, Value=%+v", event.Name, refVal, refVal) // <-- Add Log
								}
							} else {
								log.Printf("DEBUG [ExtractEventDefinitions]: Payload oneOf item has NO $ref for event '%s': %+v", event.Name, firstChoice) // <-- Add Log
							}
						} else {
							log.Printf("DEBUG [ExtractEventDefinitions]: Payload oneOf item is NOT a map for event '%s': Type=%T, Value=%+v", event.Name, oneOfList[0], oneOfList[0]) // <-- Add Log
						}
					} else {
						log.Printf("DEBUG [ExtractEventDefinitions]: Payload oneOf value is NOT a list or is empty for event '%s': Type=%T, Value=%+v", event.Name, oneOfVal, oneOfVal) // <-- Add Log
					}
				} else {
					log.Printf("DEBUG [ExtractEventDefinitions]: Payload for event '%s' has NEITHER $ref NOR oneOf key: %+v", event.Name, resolvedMsg.Payload) // <-- Add Log
				}
			} else {
				errMsg := "resolvedMsg is nil"
				if resolvedMsg != nil { // Implies Payload is nil
					errMsg = "resolvedMsg.Payload is nil"
				}
				log.Printf("DEBUG [ExtractEventDefinitions]: Skipping message schema ref extraction for event '%s' (%s)", event.Name, errMsg) // <-- Add Log
			}
			// --- End store message schema ref ---
		}
	}

	return definitions, messageSchemaRefs, nil
}

// ExtractPayloadSchemasAndRefs extracts payload schemas and their reference paths
func (l *SchemaLoader) ExtractPayloadSchemasAndRefs() (map[string]map[string]interface{}, map[string]string, error) {
	payloadSchemas := make(map[string]map[string]interface{})
	payloadRefs := make(map[string]string)
	var encounteredErrors []string // Collect errors instead of returning on first one

	// Check if schema is loaded
	if l.schema == nil {
		return payloadSchemas, payloadRefs, fmt.Errorf("no AsyncAPI schema loaded")
	}
	if l.schema.Components.Messages == nil {
		// If messages component is nil, we can't extract payloads from refs
		log.Printf("Warning: Components.Messages is nil, cannot extract payload references.")
		// Continue to process potential inline messages if any exist, but likely none will be found.
	}

	// Process each channel in the structured schema
	for channelName, channel := range l.schema.Channels {
		// Process publish operation
		if channel.Publish != nil && channel.Publish.Message != nil {
			var msgToProcess *Message
			resolvedMsgName, err := l.resolveMessageRef(channel.Publish.Message) // Get name or ""
			if err != nil {
				msg := fmt.Sprintf("channel %s (publish): failed to resolve message ref: %v", channelName, err)
				log.Printf("Warning: %s", msg)
				encounteredErrors = append(encounteredErrors, msg)
				continue // Skip this operation
			}

			if resolvedMsgName != "" { // It's a reference, look it up directly
				if l.schema.Components.Messages == nil { // Check again just in case
					msg := fmt.Sprintf("channel %s (publish): Components.Messages is nil, cannot lookup resolved message '%s'", channelName, resolvedMsgName)
					log.Printf("Warning: %s", msg)
					encounteredErrors = append(encounteredErrors, msg)
					continue
				}
				resolvedMsgCopy, found := l.schema.Components.Messages[resolvedMsgName]
				if !found {
					// This should have been caught by resolveMessageRef, but double-check
					msg := fmt.Sprintf("channel %s (publish): resolved message '%s' not found in map", channelName, resolvedMsgName)
					log.Printf("Warning: %s", msg)
					encounteredErrors = append(encounteredErrors, msg)
					continue
				}
				log.Printf("DEBUG [ExtractPayloadSchemasAndRefs]: Directly accessing map for %s (publish) using key '%s'. Payload: %+v", channelName, resolvedMsgName, resolvedMsgCopy.Payload)
				msgToProcess = &resolvedMsgCopy // Use pointer to the copy retrieved here
			} else { // It's an inline message
				msgToProcess = channel.Publish.Message
				log.Printf("DEBUG [ExtractPayloadSchemasAndRefs]: Processing inline message for %s (publish). Payload: %+v", channelName, msgToProcess.Payload)
			}

			// Call extractMessagePayload with the message obtained here
			err = extractMessagePayload(channelName, "publish", msgToProcess, payloadSchemas, payloadRefs)
			if err != nil {
				msg := fmt.Sprintf("channel %s (publish): %v", channelName, err)
				log.Printf("Warning: %s", msg)
				encounteredErrors = append(encounteredErrors, msg)
			}
		}

		// Process subscribe operation (similar logic)
		if channel.Subscribe != nil && channel.Subscribe.Message != nil {
			var msgToProcess *Message
			resolvedMsgName, err := l.resolveMessageRef(channel.Subscribe.Message) // Get name or ""
			if err != nil {
				msg := fmt.Sprintf("channel %s (subscribe): failed to resolve message ref: %v", channelName, err)
				log.Printf("Warning: %s", msg)
				encounteredErrors = append(encounteredErrors, msg)
				continue // Skip this operation
			}

			if resolvedMsgName != "" { // It's a reference, look it up directly
				if l.schema.Components.Messages == nil { // Check again just in case
					msg := fmt.Sprintf("channel %s (subscribe): Components.Messages is nil, cannot lookup resolved message '%s'", channelName, resolvedMsgName)
					log.Printf("Warning: %s", msg)
					encounteredErrors = append(encounteredErrors, msg)
					continue
				}
				resolvedMsgCopy, found := l.schema.Components.Messages[resolvedMsgName]
				if !found {
					msg := fmt.Sprintf("channel %s (subscribe): resolved message '%s' not found in map", channelName, resolvedMsgName)
					log.Printf("Warning: %s", msg)
					encounteredErrors = append(encounteredErrors, msg)
					continue
				}
				log.Printf("DEBUG [ExtractPayloadSchemasAndRefs]: Directly accessing map for %s (subscribe) using key '%s'. Payload: %+v", channelName, resolvedMsgName, resolvedMsgCopy.Payload)
				msgToProcess = &resolvedMsgCopy // Use pointer to the copy retrieved here
			} else { // It's an inline message
				msgToProcess = channel.Subscribe.Message
				log.Printf("DEBUG [ExtractPayloadSchemasAndRefs]: Processing inline message for %s (subscribe). Payload: %+v", channelName, msgToProcess.Payload)
			}

			// Call extractMessagePayload with the message obtained here
			err = extractMessagePayload(channelName, "subscribe", msgToProcess, payloadSchemas, payloadRefs)
			if err != nil {
				msg := fmt.Sprintf("channel %s (subscribe): %v", channelName, err)
				log.Printf("Warning: %s", msg)
				encounteredErrors = append(encounteredErrors, msg)
			}
		}
	}

	// Return collected errors if any occurred
	if len(encounteredErrors) > 0 {
		// Log final summary of errors
		log.Printf("Warning: Encountered %d errors during payload extraction.", len(encounteredErrors))
		// Optionally return the full error string:
		// return payloadSchemas, payloadRefs, fmt.Errorf("encountered errors during payload extraction:\n- %s", strings.Join(encounteredErrors, "\n- "))
	}

	// Return nil error even if non-fatal warnings occurred, rely on logs
	return payloadSchemas, payloadRefs, nil
}

// Helper function to resolve a message reference if present
// Returns the resolved message name (key in Components.Messages) or empty string for inline, and an error.
func (l *SchemaLoader) resolveMessageRef(message *Message) (string, error) {
	if message == nil {
		return "", fmt.Errorf("message definition is nil")
	}

	if message.Ref != "" { // Check if the Message struct itself represents a ref
		if !strings.HasPrefix(message.Ref, "#/components/messages/") {
			return "", fmt.Errorf("unsupported message reference format: %s", message.Ref)
		}
		messageName := strings.TrimPrefix(message.Ref, "#/components/messages/")

		// Ensure schema and components are loaded
		if l.schema == nil {
			log.Printf("ERROR [resolveMessageRef]: l.schema is nil when resolving '%s'", message.Ref)
			return "", fmt.Errorf("schema not loaded for resolving message ref '%s'", message.Ref)
		}
		if l.schema.Components.Messages == nil {
			log.Printf("ERROR [resolveMessageRef]: l.schema.Components.Messages is nil when resolving '%s'", message.Ref)
			return "", fmt.Errorf("components.messages not loaded for resolving message ref '%s'", message.Ref)
		}

		// --- Logging ---
		log.Printf("DEBUG [resolveMessageRef]: About to look up '%s' in Components.Messages.", messageName)
		if entry, exists := l.schema.Components.Messages[messageName]; exists {
			log.Printf("DEBUG [resolveMessageRef]:   Direct map access for '%s' shows Payload: %+v", messageName, entry.Payload)
		} else {
			log.Printf("DEBUG [resolveMessageRef]:   Message '%s' NOT FOUND in map before lookup.", messageName)
			return "", fmt.Errorf("message '%s' not found in components (from ref '%s')", messageName, message.Ref) // Return error if not found
		}
		// --- End Logging ---

		// Return the name (key) instead of a pointer to a copy
		return messageName, nil
	}

	// If not a reference (message.Ref is empty), return empty string (indicating inline)
	log.Printf("DEBUG [resolveMessageRef]: Message is inline (no $ref). Payload map: %+v", message.Payload)
	return "", nil // Empty string signifies inline message
}

// extractMessagePayload extracts payload from message definition
func extractMessagePayload(channelName, operation string, message *Message, // Changed parameter type
	payloadSchemas map[string]map[string]interface{}, payloadRefs map[string]string) error { // Return error only on failure

	// Check if message object is valid
	if message == nil {
		// This can happen if message ref resolution failed earlier
		// Logged previously, so just return nil (not an error for *this* function)
		return nil
	}

	// Try to extract entity and action from channel name first (more reliable)
	topicPrefix := config.GetEnv("EVENTS_KAFKA_TOPIC_PREFIX", "nameniac.events") // TODO: Pass config properly
	entityType, action := inferEntityActionFromChannel(channelName, topicPrefix)

	if entityType == "" || action == "" {
		// Logged by inferEntityActionFromChannel, treat as non-fatal for this extraction step
		return fmt.Errorf("could not determine entity/action from channel '%s'", channelName) // Return actual error
	}

	// Create key for schema maps
	key := fmt.Sprintf("%s.%s", entityType, action)

	// Check for payload definition within the message struct
	if message.Payload == nil {
		log.Printf("DEBUG: No payload field found for message in channel %s (%s)", channelName, key)
		return nil // No payload is not an error
	}

	payload := message.Payload                                    // Access payload map directly
	log.Printf("DEBUG [%s]: Checking payload: %+v", key, payload) // Added detailed log

	// Check if it's a reference
	if refVal, refExists := payload["$ref"]; refExists {
		log.Printf("DEBUG [%s]: Found '$ref' key. Value: %T %v", key, refVal, refVal)
		if ref, ok := refVal.(string); ok {
			log.Printf("DEBUG [%s]: '$ref' is a string: %s", key, ref)
			actualSchemaName, err := parseActualSchemaName(ref)
			if err != nil {
				// Return the parsing error
				return fmt.Errorf("parsing payload ref for %s: %w", key, err)
			}
			directRef := fmt.Sprintf("#/components/schemas/%s", actualSchemaName)
			payloadRefs[key] = directRef
			// Store the original payload map containing the ref for now.
			// TODO: Ideally, look up the actual schema definition using directRef and store that.
			payloadSchemas[key] = payload
			log.Printf("DEBUG: Extracted payload ref for %s: %s -> %s", key, ref, directRef)
			// DO NOT return nil here. Let the function finish.
		} else {
			log.Printf("DEBUG [%s]: '$ref' key exists but is NOT a string.", key)
			// Continue checking other possibilities like oneOf
		}
	} else {
		log.Printf("DEBUG [%s]: '$ref' key does NOT exist.", key)
		// Continue checking other possibilities like oneOf
	}

	// Handle oneOf only if $ref wasn't found or wasn't a string
	// (Assuming a payload won't have both a top-level $ref and a top-level oneOf)
	if _, refFound := payloadRefs[key]; !refFound { // Check if we already found a ref
		if oneOfVal, oneOfExists := payload["oneOf"]; oneOfExists {
			log.Printf("DEBUG [%s]: Found 'oneOf' key. Value: %T %v", key, oneOfVal, oneOfVal)
			if oneOf, ok := oneOfVal.([]interface{}); ok {
				if len(oneOf) == 0 {
					log.Printf("DEBUG: Found empty 'oneOf' in payload for %s. Treating as no specific payload schema.", key)
					// DO NOT return nil here.
				} else if len(oneOf) > 0 {
					if firstOf, ok := oneOf[0].(map[string]interface{}); ok {
						if ref, ok := firstOf["$ref"].(string); ok {
							actualSchemaName, err := parseActualSchemaName(ref)
							if err != nil {
								// Return the parsing error
								return fmt.Errorf("parsing payload ref from oneOf for %s: %w", key, err)
							}
							directRef := fmt.Sprintf("#/components/schemas/%s", actualSchemaName)
							payloadRefs[key] = directRef
							// Store the original payload map containing oneOf for now.
							payloadSchemas[key] = payload
							log.Printf("DEBUG: Extracted payload ref from oneOf for %s: %s -> %s", key, ref, directRef)
							// DO NOT return nil here.
						} else {
							log.Printf("DEBUG [%s]: 'oneOf' first element is map, but no '$ref' string found: %+v", key, firstOf)
						}
					} else {
						log.Printf("DEBUG [%s]: 'oneOf' first element is not a map[string]interface{}: %T", key, oneOf[0])
					}
				}
			} else {
				log.Printf("DEBUG [%s]: 'oneOf' key exists but is NOT a []interface{}.", key)
			}
		} else {
			log.Printf("DEBUG [%s]: 'oneOf' key does NOT exist.", key)
		}
	}

	// If, after checking $ref and oneOf, we still haven't stored a ref for this key, check for inline object.
	if _, refFound := payloadRefs[key]; !refFound {
		if typeVal, ok := payload["type"].(string); ok && typeVal == "object" {
			log.Printf("Warning: Payload for %s is an inline object definition, which is not fully supported for generation/typed handling.", key)
			// payloadSchemas[key] = payload // Optionally store for validation
			// payloadRefs[key] = "" // Indicate inline
			// DO NOT return nil here.
		} else {
			// Only log this final warning if no ref was found AND it wasn't an inline object
			log.Printf("Warning: Payload for %s is not a $ref, valid oneOf, or recognized inline object: %v", key, payload)
		}
	}

	// If we reached here, processing for this message's payload is done (or skipped).
	// Return nil indicating no error occurred during *this specific extraction*.
	return nil
}

// inferEntityActionFromChannel tries to infer entity type and action from channel name
// Updated to accept topicPrefix
func inferEntityActionFromChannel(channelName string, topicPrefix string) (string, string) {
	// Expecting format like "prefix.entity.action"
	prefixWithDot := topicPrefix + "."
	if strings.HasPrefix(channelName, prefixWithDot) {
		remaining := strings.TrimPrefix(channelName, prefixWithDot)
		parts := strings.Split(remaining, ".")
		if len(parts) == 2 { // Expecting entity.action
			return parts[0], parts[1]
		}
		log.Printf("Warning: Channel name '%s' starts with prefix '%s' but doesn't have 'entity.action' structure after it.", channelName, topicPrefix)
	}

	// Fallback: use the last two parts if prefix doesn't match or structure is wrong
	parts := strings.Split(channelName, ".")
	if len(parts) >= 2 {
		entityIdx := len(parts) - 2
		actionIdx := len(parts) - 1
		log.Printf("DEBUG: Falling back to last two parts for channel '%s': entity='%s', action='%s'", channelName, parts[entityIdx], parts[actionIdx])
		return parts[entityIdx], parts[actionIdx]
	}

	log.Printf("Warning: Could not infer entity/action from channel name '%s'", channelName)
	return "", ""
}

// extractEventFromOperation extracts event definition details
// Updated signature to accept resolved *Message and topicPrefix
func extractEventFromOperation(channelName, topic string, operation *Operation, resolvedMsg *Message, description string, topicPrefix string) (messages.EventDefinition, error) {
	// Try to extract entity type and action from channel name (expecting prefix.entity.action)
	entityType, action := inferEntityActionFromChannel(channelName, topicPrefix) // Use updated function

	if entityType == "" || action == "" {
		// If channel name parsing fails, we cannot reliably create a definition
		return messages.EventDefinition{}, fmt.Errorf("could not determine entity type and action from channel name '%s'", channelName)
	}

	// Use description from operation if available, fallback to channel description
	opDesc := description // Use channel description as base
	if operation.Description != "" {
		opDesc = operation.Description // Prefer operation description
	}

	// Use message title as name if available, otherwise construct from entity/action
	name := fmt.Sprintf("%s.%s", entityType, action) // Default name
	if resolvedMsg != nil && resolvedMsg.Title != "" {
		// Prefer message title if it exists and is meaningful
		cleanedTitle := strings.TrimSuffix(resolvedMsg.Title, ":Message")
		// Check if cleaned title matches the expected pattern
		if cleanedTitle == name {
			name = cleanedTitle // Use cleaned title if it matches pattern
		} else {
			log.Printf("DEBUG: Message title '%s' (cleaned: '%s') does not match expected name '%s' for channel '%s'. Using '%s'.", resolvedMsg.Title, cleanedTitle, name, channelName, name)
		}
	}

	return messages.EventDefinition{
		Name:        name,
		EntityType:  entityType,
		Action:      action,
		Description: opDesc,
		Topic:       topic, // Use the determined topic (binding or default prefix)
	}, nil
}

// RefreshSchemas loads and parses schemas from the AsyncAPI URL
func (r *SchemaRegistry) RefreshSchemas() error {
	// Skip everything if validation is disabled
	if !r.validationEnabled {
		log.Printf("Schema validation disabled, skipping schema refresh entirely.")
		return nil // Explicitly return nil if disabled
	}

	log.Printf("Refreshing AsyncAPI schemas from URL %s", r.asyncAPIURL)

	// Load the raw schema data first
	rawSchemaData, err := r.loader.fetchSchemaData(r.asyncAPIURL)
	if err != nil {
		log.Printf("Warning: Failed fetching schema data from %s: %v", r.asyncAPIURL, err)
		return fmt.Errorf("fetching schema data: %w", err) // Return error if fetch fails
	}

	// Parse the raw data into the full schema map
	var fullSchemaMap map[string]interface{}
	if err := json.Unmarshal(rawSchemaData, &fullSchemaMap); err != nil {
		log.Printf("Warning: Failed unmarshalling full schema map from %s: %v", r.asyncAPIURL, err)
		// Don't return error, but log it. Validation might still work partially if structure load succeeds.
	}

	// Now load it into the structured loader - for topic lookups and component access
	if err := r.loader.LoadFromJSON(rawSchemaData); err != nil {
		log.Printf("Warning: Failed loading AsyncAPI schema structure from %s: %v", r.asyncAPIURL, err)
		// Don't return error, but log it. Validation might fail later.
	}

	// --- Get topic prefix from config ---
	// TODO: Refactor to pass config properly instead of reading env directly
	topicPrefix := config.DefaultEventsConfig().KafkaTopicPrefix

	// Extract event definitions - pass the topic prefix
	// Capture all three return values: definitions, messageSchemaRefs, and error
	definitions, messageSchemaRefs, errDefs := r.loader.ExtractEventDefinitions(topicPrefix) // Pass prefix
	if errDefs != nil {
		log.Printf("Warning: Error extracting event definitions: %v", errDefs)
		// Consider if this error should be fatal or if partial data is acceptable
	}
	// Add a log for the captured message schema refs (optional debug)
	if len(messageSchemaRefs) > 0 {
		log.Printf("DEBUG: Extracted %d message schema references.", len(messageSchemaRefs))
	}

	// Extract payload schemas (raw maps) AND their original $refs
	_, payloadRefs, errPayloads := r.loader.ExtractPayloadSchemasAndRefs()
	if errPayloads != nil {
		log.Printf("Warning: Error extracting payload schemas: %v", errPayloads)
		// Continue if possible, some schemas might still be extracted
	}
	if len(payloadRefs) == 0 && errPayloads == nil {
		log.Printf("Warning: No payload references were extracted from the schema.")
	}

	// --- Update registry state ---
	r.mu.Lock()
	defer r.mu.Unlock()

	// Store definitions even if there were errors, they might be partially correct
	r.definitions = definitions
	r.rawFullSchema = fullSchemaMap
	r.payloadSchemaRefs = payloadRefs // Store the refs

	// --- Compile Schemas ---
	compiledSchemas := make(map[string]*gojsonschema.Schema)
	schemaLoaderWithContext := gojsonschema.NewSchemaLoader()
	// Add the full schema document with a base URI to allow resolving #/components/...
	errAdd := schemaLoaderWithContext.AddSchema("asyncapi:///", gojsonschema.NewGoLoader(r.rawFullSchema))
	if errAdd != nil {
		log.Printf("Error adding full schema to validator context: %v", errAdd)
		// Cannot proceed with compilation if context fails
		r.eventSchemas = make(map[string]*gojsonschema.Schema) // Clear potentially stale schemas
		// Unlock is handled by defer
		return fmt.Errorf("adding full schema context: %w", errAdd) // Return error
	}
	r.fullSchemaValidator = schemaLoaderWithContext // Store the loader

	// Compile each specific payload schema using the context loader
	for key, payloadRef := range r.payloadSchemaRefs {
		if payloadRef == "" { // Skip inline schemas or those without a ref
			log.Printf("DEBUG: Skipping compilation for key '%s' as payloadRef is empty (likely inline or not found).", key)
			continue
		}

		// Construct the full URI relative to the base URI used in AddSchema
		if !strings.HasPrefix(payloadRef, "#") {
			log.Printf("Warning: Skipping schema compilation for key '%s' due to non-local ref: '%s'", key, payloadRef)
			continue
		}
		fullRefURI := "asyncapi:///" + payloadRef // Combine base URI and the fragment identifier

		log.Printf("DEBUG: Attempting to compile schema for key '%s' using full ref URI: '%s'", key, fullRefURI)

		// Compile using NewReferenceLoader and the full URI
		compiledSchema, err := schemaLoaderWithContext.Compile(gojsonschema.NewReferenceLoader(fullRefURI))
		if err != nil {
			// Log the specific ref that failed but continue trying others
			log.Printf("Error compiling schema for %s (ref: %s, full URI: %s): %v. Check schema structure and $refs.", key, payloadRef, fullRefURI, err)
			continue
		}
		compiledSchemas[key] = compiledSchema
	}

	r.eventSchemas = compiledSchemas // Update compiled schemas map
	r.lastRefresh = time.Now()
	// Unlock is handled by defer

	log.Printf("AsyncAPI schema refreshed: %d event definitions, %d payload schemas compiled (check warnings for errors)",
		len(r.definitions), len(compiledSchemas))

	// Return nil if refresh completed, even if some compilations failed (rely on logs)
	return nil
}

// --- Add helper to fetch raw schema data ---
// (Ensure this or similar logic exists)
func (l *SchemaLoader) fetchSchemaData(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body) // Try to read body for context
		return nil, fmt.Errorf("http status %s: %s", resp.Status, string(bodyBytes))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}
	return data, nil
}

// GetSchemaComponents returns the components part of the loaded schema.
// Needed for the generator.
func (l *SchemaLoader) GetSchemaComponents() (Components, bool) {
	if l.schema == nil {
		return Components{}, false
	}
	return l.schema.Components, true
}
