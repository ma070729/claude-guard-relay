package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"
)

// Room 表示一个配对房间，包含 agent 和 phone 客户端集合
type Room struct {
	Code      string                 // 6位配对码
	Agents    map[*Client]bool       // 电脑端 agent 连接
	Phones    map[*Client]bool       // 手机端连接
	Pending   map[string]*Message    // 排队中的权限请求 (key=request_id)
	CreatedAt time.Time
	mu        sync.Mutex
}

// Hub 管理一组客户端连接和房间配对
type Hub struct {
	rooms      map[string]*Room   // code -> Room
	clients    map[*Client]bool   // 所有活跃连接
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
}

// NewHub 创建新的 Hub 实例
func NewHub() *Hub {
	return &Hub{
		rooms:      make(map[string]*Room),
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// Run 启动 Hub 主循环，处理客户端注册、注销和定期清理
func (h *Hub) Run() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case c := <-h.register:
			h.mu.Lock()
			h.clients[c] = true
			h.mu.Unlock()

		case c := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
			}
			h.mu.Unlock()
			h.removeFromRoom(c)

		case <-ticker.C:
			h.cleanupExpired()
		}
	}
}

// Register 非阻塞地将客户端注册到 hub
func (h *Hub) Register(c *Client) {
	select {
	case h.register <- c:
	default:
		log.Println("hub: register channel full, dropping client")
	}
}

// Unregister 非阻塞地将客户端从 hub 注销
func (h *Hub) Unregister(c *Client) {
	select {
	case h.unregister <- c:
	default:
		log.Println("hub: unregister channel full, dropping client")
	}
}

// HandleMessage 根据消息类型分发到对应的处理方法
func (h *Hub) HandleMessage(c *Client, msg *Message) {
	switch msg.Type {
	case TypePairingRequest:
		h.handlePairing(c, msg)
	case TypePermissionRequest:
		h.routeToPhones(c, msg)
	case TypePermissionResponse:
		h.routeToAgent(c, msg)
	default:
		log.Printf("hub: unknown message type from client: %s", msg.Type)
	}
}

// handlePairing 处理配对请求
func (h *Hub) handlePairing(c *Client, msg *Message) {
	if msg.PairCode != "" {
		// 手机配对：通过配对码加入已有房间
		h.mu.RLock()
		room, ok := h.rooms[msg.PairCode]
		h.mu.RUnlock()

		if !ok {
			log.Printf("hub: pairing code not found: %s", msg.PairCode)
			resp := NewPairingResponse(msg.PairCode, "配对码不存在或已过期")
			c.Send(&resp)
			return
		}

		room.mu.Lock()
		room.Phones[c] = true
		room.mu.Unlock()

		c.mu.Lock()
		c.roomID = msg.PairCode
		c.role = RolePhone
		c.mu.Unlock()

		// 补发 Pending 中的所有排队请求
		room.mu.Lock()
		for _, pending := range room.Pending {
			c.Send(pending)
		}
		room.mu.Unlock()

		// 发送配对成功响应
		resp := NewPairingResponse(msg.PairCode, "")
		c.Send(&resp)

		log.Printf("hub: phone joined room %s", msg.PairCode)
	} else {
		// agent 请求新配对码
		code := generateCode()

		room := &Room{
			Code:      code,
			Agents:    make(map[*Client]bool),
			Phones:    make(map[*Client]bool),
			Pending:   make(map[string]*Message),
			CreatedAt: time.Now(),
		}
		room.Agents[c] = true

		h.mu.Lock()
		h.rooms[code] = room
		h.mu.Unlock()

		c.mu.Lock()
		c.roomID = code
		c.role = RoleAgent
		c.mu.Unlock()

		// 返回配对码给 agent
		resp := NewPairingResponse(code, "")
		c.Send(&resp)

		log.Printf("hub: agent created room %s", code)
	}
}

// routeToPhones 将权限请求路由到房间内所有手机
func (h *Hub) routeToPhones(c *Client, msg *Message) {
	h.mu.RLock()
	room, ok := h.rooms[c.roomID]
	h.mu.RUnlock()

	if !ok {
		log.Printf("hub: room not found for client in routeToPhones: %s", c.roomID)
		return
	}

	// 将消息加入 Pending 队列
	room.mu.Lock()
	room.Pending[msg.RequestID] = msg
	phones := make([]*Client, 0, len(room.Phones))
	for p := range room.Phones {
		phones = append(phones, p)
	}
	room.mu.Unlock()

	// 如果没有手机在线，消息留在 Pending 中等待补发
	for _, p := range phones {
		p.Send(msg)
	}
}

// routeToAgent 将权限响应路由到目标 agent
func (h *Hub) routeToAgent(c *Client, msg *Message) {
	h.mu.RLock()
	room, ok := h.rooms[c.roomID]
	h.mu.RUnlock()

	if !ok {
		log.Printf("hub: room not found for client in routeToAgent: %s", c.roomID)
		return
	}

	// 从 Pending 中删除已处理的请求
	room.mu.Lock()
	delete(room.Pending, msg.RequestID)
	room.mu.Unlock()

	agent := h.findAgent(room, msg.SessionID)
	if agent == nil {
		log.Printf("hub: no agent found in room %s", c.roomID)
		return
	}

	agent.Send(msg)
}

// findAgent 在房间中查找目标 agent
func (h *Hub) findAgent(room *Room, sessionID string) *Client {
	room.mu.Lock()
	defer room.mu.Unlock()

	if len(room.Agents) == 0 {
		return nil
	}

	// 优先匹配 sessionID
	for a := range room.Agents {
		if a.sessionID == sessionID {
			return a
		}
	}

	// fallback: 返回任意一个 agent
	for a := range room.Agents {
		return a
	}

	return nil
}

// removeFromRoom 将客户端从其房间中移除，并清理空房间
func (h *Hub) removeFromRoom(c *Client) {
	roomID := c.roomID
	if roomID == "" {
		return
	}

	h.mu.RLock()
	room, ok := h.rooms[roomID]
	h.mu.RUnlock()

	if !ok {
		return
	}

	room.mu.Lock()
	delete(room.Agents, c)
	delete(room.Phones, c)

	// 如果房间中 Agents 和 Phones 都为空，清理房间
	empty := len(room.Agents) == 0 && len(room.Phones) == 0
	room.mu.Unlock()

	if empty {
		h.mu.Lock()
		// 双重检查：防止竞态条件
		room.mu.Lock()
		stillEmpty := len(room.Agents) == 0 && len(room.Phones) == 0
		room.mu.Unlock()

		if stillEmpty {
			delete(h.rooms, roomID)
			log.Printf("hub: removed empty room %s", roomID)
		}
		h.mu.Unlock()
	}
}

// cleanupExpired 清理过期的 Pending 请求和长时间无连接的房间
func (h *Hub) cleanupExpired() {
	now := time.Now()
	pendingTimeout := 5 * time.Minute
	roomTimeout := 24 * time.Hour

	h.mu.RLock()
	rooms := make([]*Room, 0, len(h.rooms))
	for _, r := range h.rooms {
		rooms = append(rooms, r)
	}
	h.mu.RUnlock()

	for _, room := range rooms {
		room.mu.Lock()

		// 清理超过 5 分钟未处理的 Pending 请求
		for id, msg := range room.Pending {
			msgTime := time.Unix(msg.Timestamp, 0)
			if now.Sub(msgTime) > pendingTimeout {
				delete(room.Pending, id)
			}
		}

		isEmpty := len(room.Agents) == 0 && len(room.Phones) == 0
		isExpired := now.Sub(room.CreatedAt) > roomTimeout
		room.mu.Unlock()

		// 清理超过 24 小时且无连接的房间
		if isEmpty && isExpired {
			h.mu.Lock()
			room.mu.Lock()
			stillEmpty := len(room.Agents) == 0 && len(room.Phones) == 0
			room.mu.Unlock()

			if stillEmpty {
				delete(h.rooms, room.Code)
				log.Printf("hub: cleaned up expired room %s", room.Code)
			}
			h.mu.Unlock()
		}
	}
}

// generateCode 使用 crypto/rand 生成 6 位数字字符串
func generateCode() string {
	n, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		// fallback: 极低概率，使用时间戳后 6 位兜底
		log.Printf("hub: crypto/rand failed, using fallback: %v", err)
		return fmt.Sprintf("%06d", time.Now().UnixNano()%1000000)
	}
	return fmt.Sprintf("%06d", n.Int64())
}
