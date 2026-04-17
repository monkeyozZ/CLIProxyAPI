package helps

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"strings"
)

const (
	kiroEventPreludeSize    = 12
	kiroEventMinimumMessage = kiroEventPreludeSize + 4
	kiroEventMaximumMessage = 16 * 1024 * 1024
)

const (
	KiroEventAssistantResponse = "assistant_response"
	KiroEventToolUse           = "tool_use"
	KiroEventContextUsage      = "context_usage"
)

type KiroEventStreamFrame struct {
	MessageType   string
	EventType     string
	ErrorCode     string
	ExceptionType string
	Payload       []byte
}

type KiroAssistantResponseEvent struct {
	Content string `json:"content"`
}

type KiroToolUseEvent struct {
	Name      string `json:"name"`
	ToolUseID string `json:"toolUseId"`
	Input     string `json:"input"`
	Stop      bool   `json:"stop"`
}

type KiroContextUsageEvent struct {
	ContextUsagePercentage float64 `json:"contextUsagePercentage"`
}

type KiroEvent struct {
	Type              string
	AssistantResponse *KiroAssistantResponseEvent
	ToolUse           *KiroToolUseEvent
	ContextUsage      *KiroContextUsageEvent
}

func DecodeKiroEventStream(data []byte) ([]KiroEventStreamFrame, error) {
	frames := make([]KiroEventStreamFrame, 0, 16)
	for offset := 0; offset < len(data); {
		if len(data)-offset < kiroEventPreludeSize {
			return nil, fmt.Errorf("kiro event stream ended mid-frame")
		}
		totalLength := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		headerLength := int(binary.BigEndian.Uint32(data[offset+4 : offset+8]))
		preludeCRC := binary.BigEndian.Uint32(data[offset+8 : offset+12])
		if totalLength < kiroEventMinimumMessage {
			return nil, fmt.Errorf("kiro event stream frame too small: %d", totalLength)
		}
		if totalLength > kiroEventMaximumMessage {
			return nil, fmt.Errorf("kiro event stream frame too large: %d", totalLength)
		}
		if offset+totalLength > len(data) {
			return nil, fmt.Errorf("kiro event stream truncated frame")
		}
		if actual := crc32.ChecksumIEEE(data[offset : offset+8]); actual != preludeCRC {
			return nil, fmt.Errorf("kiro event stream prelude crc mismatch")
		}
		frameData := data[offset : offset+totalLength]
		messageCRC := binary.BigEndian.Uint32(frameData[totalLength-4:])
		if actual := crc32.ChecksumIEEE(frameData[:totalLength-4]); actual != messageCRC {
			return nil, fmt.Errorf("kiro event stream message crc mismatch")
		}
		headersStart := kiroEventPreludeSize
		headersEnd := headersStart + headerLength
		if headersEnd > totalLength-4 {
			return nil, fmt.Errorf("kiro event stream invalid header length")
		}
		headers, err := parseKiroEventHeaders(frameData[headersStart:headersEnd])
		if err != nil {
			return nil, err
		}
		payload := append([]byte(nil), frameData[headersEnd:totalLength-4]...)
		frames = append(frames, KiroEventStreamFrame{
			MessageType:   headers[":message-type"],
			EventType:     headers[":event-type"],
			ErrorCode:     headers[":error-code"],
			ExceptionType: headers[":exception-type"],
			Payload:       payload,
		})
		offset += totalLength
	}
	return frames, nil
}

func CollectKiroEventsFromEventStream(data []byte) ([]KiroEvent, error) {
	frames, err := DecodeKiroEventStream(data)
	if err != nil {
		return nil, err
	}
	events := make([]KiroEvent, 0, len(frames))
	for _, frame := range frames {
		switch frame.MessageType {
		case "error":
			msg := strings.TrimSpace(string(frame.Payload))
			if frame.ErrorCode != "" {
				return nil, fmt.Errorf("kiro upstream error %s: %s", frame.ErrorCode, msg)
			}
			return nil, fmt.Errorf("kiro upstream error: %s", msg)
		case "exception":
			msg := strings.TrimSpace(string(frame.Payload))
			if frame.ExceptionType != "" {
				return nil, fmt.Errorf("kiro upstream exception %s: %s", frame.ExceptionType, msg)
			}
			return nil, fmt.Errorf("kiro upstream exception: %s", msg)
		}

		switch frame.EventType {
		case "assistantResponseEvent":
			var payload KiroAssistantResponseEvent
			if err := json.Unmarshal(frame.Payload, &payload); err != nil {
				return nil, fmt.Errorf("kiro assistant payload decode failed: %w", err)
			}
			events = append(events, KiroEvent{
				Type:              KiroEventAssistantResponse,
				AssistantResponse: &payload,
			})
		case "toolUseEvent":
			var payload KiroToolUseEvent
			if err := json.Unmarshal(frame.Payload, &payload); err != nil {
				return nil, fmt.Errorf("kiro tool payload decode failed: %w", err)
			}
			events = append(events, KiroEvent{
				Type:    KiroEventToolUse,
				ToolUse: &payload,
			})
		case "contextUsageEvent":
			var payload KiroContextUsageEvent
			if err := json.Unmarshal(frame.Payload, &payload); err != nil {
				return nil, fmt.Errorf("kiro context usage payload decode failed: %w", err)
			}
			events = append(events, KiroEvent{
				Type:         KiroEventContextUsage,
				ContextUsage: &payload,
			})
		}
	}
	return events, nil
}

func CollectKiroTextFromEventStream(data []byte) (string, error) {
	events, err := CollectKiroEventsFromEventStream(data)
	if err != nil {
		return "", err
	}
	var builder strings.Builder
	for _, event := range events {
		if event.AssistantResponse == nil {
			continue
		}
		builder.WriteString(event.AssistantResponse.Content)
	}
	return builder.String(), nil
}

func parseKiroEventHeaders(data []byte) (map[string]string, error) {
	headers := make(map[string]string)
	for offset := 0; offset < len(data); {
		nameLen := int(data[offset])
		offset++
		if nameLen <= 0 || offset+nameLen > len(data) {
			return nil, fmt.Errorf("kiro event stream invalid header name")
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen
		if offset >= len(data) {
			return nil, fmt.Errorf("kiro event stream missing header type")
		}
		valueType := data[offset]
		offset++
		value, consumed, err := parseKiroHeaderValue(data[offset:], valueType)
		if err != nil {
			return nil, err
		}
		offset += consumed
		if value != "" {
			headers[name] = value
		}
	}
	return headers, nil
}

func parseKiroHeaderValue(data []byte, valueType byte) (string, int, error) {
	switch valueType {
	case 0, 1:
		return "", 0, nil
	case 2:
		if len(data) < 1 {
			return "", 0, fmt.Errorf("kiro event stream short byte header")
		}
		return "", 1, nil
	case 3:
		if len(data) < 2 {
			return "", 0, fmt.Errorf("kiro event stream short short header")
		}
		return "", 2, nil
	case 4:
		if len(data) < 4 {
			return "", 0, fmt.Errorf("kiro event stream short integer header")
		}
		return "", 4, nil
	case 5, 8:
		if len(data) < 8 {
			return "", 0, fmt.Errorf("kiro event stream short long header")
		}
		return "", 8, nil
	case 6, 7:
		if len(data) < 2 {
			return "", 0, fmt.Errorf("kiro event stream short variable header")
		}
		length := int(binary.BigEndian.Uint16(data[:2]))
		if len(data) < 2+length {
			return "", 0, fmt.Errorf("kiro event stream truncated variable header")
		}
		if valueType == 7 {
			return string(data[2 : 2+length]), 2 + length, nil
		}
		return "", 2 + length, nil
	case 9:
		if len(data) < 16 {
			return "", 0, fmt.Errorf("kiro event stream short uuid header")
		}
		return "", 16, nil
	default:
		return "", 0, fmt.Errorf("kiro event stream invalid header type: %d", valueType)
	}
}
