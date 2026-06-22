package main

import (
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 30 * time.Second
	pingPeriod     = 15 * time.Second
	maxMessageSize = 4096
)

// Client 表示一个 WebSocket 客户端连接
type Client struct {
	hub       *Hub
	conn      *websocket.Conn
	send      chan []byte
	roomID    string
	sessionID string
	role      Role
	mu        sync.Mutex
}

// NewClient 创建新的客户端实例
func NewClient(hub *Hub, conn *websocket.Conn) *Client {
	return &Client{
		hub:  hub,
		conn: conn,
		send: make(chan []byte, 32),
	}
}

// ReadLoop 循环读取客户端消息
func (c *Client) ReadLoop() {
	defer func() {
		c.hub.Unregister(c)
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(
				err, websocket.CloseGoingAway, websocket.CloseNormalClosure,
			) {
				log.Printf("client read error: %v", err)
			}
			break
		}

		msg, err := DecodeMessage(data)
		if err != nil {
			log.Printf("decode error: %v", err)
			continue
		}

		c.hub.HandleMessage(c, &msg)
	}
}

// WriteLoop 循环向客户端写入消息
func (c *Client) WriteLoop() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// Send 非阻塞地将消息发送到客户端的发送通道
// 如果通道已满，消息会被丢弃（返回 nil）
func (c *Client) Send(msg *Message) error {
	data, err := msg.Encode()
	if err != nil {
		return err
	}
	select {
	case c.send <- data:
	default:
		// 发送缓冲区满，丢弃消息以避免阻塞
	}
	return nil
}
