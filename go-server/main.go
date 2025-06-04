package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Session represents a single call session
// Each call is associated with a Twilio connection, an OpenAI connection, and a set of log subscribers
// This struct is simplified for demo purposes

type Session struct {
	id           string
	twilioConn   *websocket.Conn
	openaiConn   *websocket.Conn
	logConns     map[*websocket.Conn]struct{}
	streamSid    string
	savedConfig  map[string]interface{}
	mu           sync.Mutex
	openaiAPIKey string
}

var (
	upgrader   = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	sessions   = make(map[string]*Session)
	sessionsMu sync.Mutex
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}
	publicURL := os.Getenv("PUBLIC_URL")
	openaiAPIKey := os.Getenv("OPENAI_API_KEY")
	if openaiAPIKey == "" {
		log.Fatal("OPENAI_API_KEY environment variable is required")
	}

	http.HandleFunc("/public-url", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"publicUrl": publicURL})
	})

	http.HandleFunc("/twiml", func(w http.ResponseWriter, r *http.Request) {
		wsURL := fmt.Sprintf("%s/call", publicURL)
		resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>\n<Response>\n  <Say>Connected</Say>\n  <Connect>\n    <Stream url="%s" />\n  </Connect>\n  <Say>Disconnected</Say>\n</Response>`, wsURL)
		w.Header().Set("Content-Type", "text/xml")
		fmt.Fprint(w, resp)
	})

	http.HandleFunc("/tools", func(w http.ResponseWriter, r *http.Request) {
		schema := map[string]interface{}{
			"name":        "get_weather_from_coords",
			"type":        "function",
			"description": "Get the current weather",
			"parameters": map[string]interface{}{
				"type": "object",
				"properties": map[string]map[string]string{
					"latitude":  {"type": "number"},
					"longitude": {"type": "number"},
				},
				"required": []string{"latitude", "longitude"},
			},
		}
		json.NewEncoder(w).Encode([]interface{}{schema})
	})

	http.HandleFunc("/call", func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("upgrade:", err)
			return
		}

		id := uuid.New().String()
		s := &Session{id: id, twilioConn: ws, logConns: make(map[*websocket.Conn]struct{}), openaiAPIKey: openaiAPIKey}
		sessionsMu.Lock()
		sessions[id] = s
		sessionsMu.Unlock()
		go handleTwilio(s)
	})

	http.HandleFunc("/logs", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "missing session id", http.StatusBadRequest)
			return
		}

		sessionsMu.Lock()
		s := sessions[id]
		sessionsMu.Unlock()
		if s == nil {
			http.Error(w, "session not found", http.StatusNotFound)
			return
		}

		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("upgrade:", err)
			return
		}
		s.mu.Lock()
		s.logConns[ws] = struct{}{}
		s.mu.Unlock()
		go func() {
			for {
				_, _, err := ws.ReadMessage()
				if err != nil {
					break
				}
			}
			s.mu.Lock()
			delete(s.logConns, ws)
			s.mu.Unlock()
			ws.Close()
		}()
	})

	log.Printf("Server running on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleTwilio(s *Session) {
	defer func() {
		s.twilioConn.Close()
		sessionsMu.Lock()
		delete(sessions, s.id)
		sessionsMu.Unlock()
		if s.openaiConn != nil {
			s.openaiConn.Close()
		}
		s.mu.Lock()
		for conn := range s.logConns {
			conn.Close()
		}
		s.mu.Unlock()
	}()

	for {
		_, data, err := s.twilioConn.ReadMessage()
		if err != nil {
			log.Println("twilio read error:", err)
			return
		}
		var msg map[string]interface{}
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		if event, ok := msg["event"].(string); ok {
			switch event {
			case "start":
				if start, ok := msg["start"].(map[string]interface{}); ok {
					if sid, ok := start["streamSid"].(string); ok {
						s.streamSid = sid
					}
				}
				go connectOpenAI(s)
			case "media":
				if audio, ok := msg["media"].(map[string]interface{}); ok {
					if payload, ok := audio["payload"].(string); ok {
						s.mu.Lock()
						if s.openaiConn != nil {
							out := map[string]interface{}{
								"type":  "input_audio_buffer.append",
								"audio": payload,
							}
							s.openaiConn.WriteJSON(out)
						}
						s.mu.Unlock()
					}
				}
			case "close":
				return
			}
		}
	}
}

func connectOpenAI(s *Session) {
	if s.openaiConn != nil {
		return
	}
	header := http.Header{}
	header.Set("Authorization", "Bearer "+s.openaiAPIKey)
	header.Set("OpenAI-Beta", "realtime=v1")
	c, _, err := websocket.DefaultDialer.Dial("wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-12-17", header)
	if err != nil {
		log.Println("openai dial error:", err)
		return
	}
	s.mu.Lock()
	s.openaiConn = c
	s.mu.Unlock()
	go handleOpenAI(s)
}

func handleOpenAI(s *Session) {
	for {
		_, data, err := s.openaiConn.ReadMessage()
		if err != nil {
			log.Println("openai read error:", err)
			return
		}
		s.mu.Lock()
		for conn := range s.logConns {
			conn.WriteMessage(websocket.TextMessage, data)
		}
		s.mu.Unlock()

		var msg map[string]interface{}
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		if msg["type"] == "response.audio.delta" {
			s.mu.Lock()
			if s.twilioConn != nil {
				out := map[string]interface{}{
					"event":     "media",
					"streamSid": s.streamSid,
					"media": map[string]string{
						"payload": fmt.Sprint(msg["delta"]),
					},
				}
				s.twilioConn.WriteJSON(out)
				mark := map[string]string{"event": "mark", "streamSid": s.streamSid}
				s.twilioConn.WriteJSON(mark)
			}
			s.mu.Unlock()
		}
	}
}
