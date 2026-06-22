package main

import (
	"encoding/json"
	"fmt"
)

// MessageType 表示消息类型
type MessageType string

const (
	TypePermissionRequest  MessageType = "permission_request"
	TypePermissionResponse MessageType = "permission_response"
	TypePairingRequest     MessageType = "pairing_request"
	TypePairingResponse    MessageType = "pairing_response"
	TypeHeartbeat          MessageType = "heartbeat"
	TypeError              MessageType = "error"
)

// Role 表示客户端角色
type Role string

const (
	RoleAgent Role = "agent"
	RolePhone Role = "phone"
)

// Message 表示一条 WebSocket 消息
type Message struct {
	Type      MessageType `json:"type"`
	RequestID string      `json:"request_id,omitempty"`
	SessionID string      `json:"session_id,omitempty"`
	Project   string      `json:"project,omitempty"`
	Tool      string      `json:"tool,omitempty"`
	Command   string      `json:"command,omitempty"`
	Options   []string    `json:"options,omitempty"`
	Action    string      `json:"action,omitempty"`
	PairCode  string      `json:"pair_code,omitempty"`
	Role      Role        `json:"role,omitempty"`
	Timestamp int64       `json:"timestamp,omitempty"`
	Error     string      `json:"error,omitempty"`
}

// NewPermissionRequest 创建权限请求消息
func NewPermissionRequest(requestID, sessionID, project, tool, command string, timestamp int64) Message {
	return Message{
		Type:      TypePermissionRequest,
		RequestID: requestID,
		SessionID: sessionID,
		Project:   project,
		Tool:      tool,
		Command:   command,
		Options:   []string{"allow", "deny", "always_allow", "always_deny"},
		Timestamp: timestamp,
	}
}

// NewPermissionResponse 创建权限响应消息
func NewPermissionResponse(requestID, action, sessionID string) Message {
	return Message{
		Type:      TypePermissionResponse,
		RequestID: requestID,
		Action:    action,
		SessionID: sessionID,
	}
}

// NewPairingRequest 创建配对请求消息
func NewPairingRequest(pairCode string, role Role) Message {
	return Message{
		Type:     TypePairingRequest,
		PairCode: pairCode,
		Role:     role,
	}
}

// NewPairingResponse 创建配对响应消息
func NewPairingResponse(pairCode, errMsg string) Message {
	msg := Message{
		Type:     TypePairingResponse,
		PairCode: pairCode,
	}
	if errMsg != "" {
		msg.Error = errMsg
	}
	return msg
}

// NewHeartbeat 创建心跳消息
func NewHeartbeat() Message {
	return Message{Type: TypeHeartbeat}
}

// NewError 创建错误消息
func NewError(errMsg string) Message {
	return Message{
		Type:  TypeError,
		Error: errMsg,
	}
}

// Encode 将 Message 序列化为 JSON 字节切片
func (m Message) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("encode message: %w", err)
	}
	return data, nil
}

// DecodeMessage 将 JSON 字节切片反序列化为 Message
func DecodeMessage(data []byte) (Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return Message{}, fmt.Errorf("decode message: %w", err)
	}
	return msg, nil
}
