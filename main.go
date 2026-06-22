package main

import (
	"log"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
)

func main() {
	// 从 PORT 环境变量读取端口，默认 "8080"
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// 创建 Hub 并启动主循环
	hub := NewHub()
	go hub.Run()

	// 配置 WebSocket Upgrader
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	// /ws 路径：WebSocket 升级
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("websocket upgrade error: %v", err)
			return
		}

		client := NewClient(hub, conn)
		hub.Register(client)

		go client.ReadLoop()
		go client.WriteLoop()
	})

	// /health 路径：健康检查
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// 启动 HTTP 服务器
	log.Printf("relay-server starting on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
