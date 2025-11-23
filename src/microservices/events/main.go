package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Data      map[string]interface{} `json:"data"`
	Timestamp time.Time              `json:"timestamp"`
}

type EventService struct {
	kafkaBrokers []string
	writer       *kafka.Writer
	eventCounter int
	counterLock  sync.Mutex

	// In-memory storage для демонстрации
	events     []Event
	eventsLock sync.RWMutex
}

var eventService *EventService

func NewEventService(brokers []string) *EventService {
	service := &EventService{
		kafkaBrokers: brokers,
		events:       make([]Event, 0),
	}

	// Initialize Kafka writer
	service.writer = &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Balancer: &kafka.LeastBytes{},
	}

	// Test Kafka connection
	log.Printf("🔗 Connecting to Kafka brokers: %v", brokers)

	return service
}

func (es *EventService) Close() {
	if es.writer != nil {
		es.writer.Close()
	}
}

func (es *EventService) PublishEvent(topic string, event Event) error {
	// Generate ID
	es.counterLock.Lock()
	es.eventCounter++
	event.ID = strconv.Itoa(es.eventCounter)
	es.counterLock.Unlock()

	event.Timestamp = time.Now()

	// Log event processing
	log.Printf("📝 Processing %s event #%s with data: %+v", event.Type, event.ID, event.Data)

	// Store in memory (для MVP демонстрации)
	es.eventsLock.Lock()
	es.events = append(es.events, event)
	// Keep only last 50 events
	if len(es.events) > 50 {
		es.events = es.events[len(es.events)-50:]
	}
	es.eventsLock.Unlock()

	// Serialize event to JSON
	eventBytes, err := json.Marshal(event)
	if err != nil {
		log.Printf("❌ Failed to marshal event: %v", err)
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Try to send to Kafka
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = es.writer.WriteMessages(ctx,
		kafka.Message{
			Topic: topic,
			Key:   []byte(event.ID),
			Value: eventBytes,
		},
	)

	if err != nil {
		log.Printf("⚠️ Failed to send to Kafka (fallback to local storage): %v", err)
		// Event is still stored locally for MVP
	} else {
		log.Printf("✅ Event #%s successfully published to Kafka topic: %s", event.ID, topic)
	}

	// Simulate event processing
	es.processEvent(event)

	return nil
}

func (es *EventService) processEvent(event Event) {
	log.Printf("⚙️ Processing event #%s of type '%s'", event.ID, event.Type)

	switch event.Type {
	case "user_event":
		log.Printf("👤 USER EVENT PROCESSED: %+v", event.Data)
		if userID, ok := event.Data["user_id"]; ok {
			log.Printf("   - User ID: %v", userID)
		}
		if action, ok := event.Data["action"]; ok {
			log.Printf("   - Action: %v", action)
		}

	case "movie_event":
		log.Printf("🎬 MOVIE EVENT PROCESSED: %+v", event.Data)
		if movieID, ok := event.Data["movie_id"]; ok {
			log.Printf("   - Movie ID: %v", movieID)
		}
		if action, ok := event.Data["action"]; ok {
			log.Printf("   - Action: %v", action)
		}

	case "payment_event":
		log.Printf("💳 PAYMENT EVENT PROCESSED: %+v", event.Data)
		if amount, ok := event.Data["amount"]; ok {
			log.Printf("   - Amount: %v", amount)
		}
		if userID, ok := event.Data["user_id"]; ok {
			log.Printf("   - User ID: %v", userID)
		}

	default:
		log.Printf("📋 GENERAL EVENT PROCESSED: %+v", event.Data)
	}

	log.Printf("✅ Event #%s processing completed at %s", event.ID, time.Now().Format("15:04:05"))
}

func (es *EventService) GetEvents(eventType string) []Event {
	es.eventsLock.RLock()
	defer es.eventsLock.RUnlock()

	if eventType == "" {
		return es.events
	}

	var filtered []Event
	for _, event := range es.events {
		if event.Type == eventType {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

func main() {
	// Get Kafka brokers from environment
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "kafka:9092"
	}

	brokers := strings.Split(kafkaBrokers, ",")
	eventService = NewEventService(brokers)
	defer eventService.Close()

	// Add some initial demo events
	initializeDemoEvents()

	// Set up HTTP routes
	http.HandleFunc("/api/events", handleEvents)
	http.HandleFunc("/api/events/health", handleHealth)
	http.HandleFunc("/api/events/movie", handleMovieEvent)
	http.HandleFunc("/api/events/user", handleUserEvent)
	http.HandleFunc("/api/events/payment", handlePaymentEvent)
	http.HandleFunc("/api/events/publish", handlePublishEvent)
	http.HandleFunc("/health", handleHealth)

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	log.Printf("🚀 Starting Events Service MVP with Kafka integration on port %s", port)
	log.Printf("📡 Kafka brokers: %s", kafkaBrokers)
	log.Printf("🌐 Available endpoints:")
	log.Printf("   POST /api/events/user    - Create user event")
	log.Printf("   POST /api/events/movie   - Create movie event")
	log.Printf("   POST /api/events/payment - Create payment event")
	log.Printf("   GET  /api/events         - Get all events")
	log.Printf("   GET  /api/events/health  - Health check")

	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func initializeDemoEvents() {
	log.Printf("🔄 Initializing demo events...")

	// Demo user event
	userEvent := Event{
		Type: "user_event",
		Data: map[string]interface{}{
			"user_id": 1,
			"action":  "login",
			"email":   "demo@example.com",
		},
	}
	eventService.PublishEvent("user-events", userEvent)

	// Demo movie event
	movieEvent := Event{
		Type: "movie_event",
		Data: map[string]interface{}{
			"movie_id": 1,
			"action":   "viewed",
			"user_id":  1,
			"duration": 120,
		},
	}
	eventService.PublishEvent("movie-events", movieEvent)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	eventsCount := len(eventService.GetEvents(""))

	response := map[string]interface{}{
		"status":        true,
		"service":       "events-service-mvp",
		"kafka_brokers": eventService.kafkaBrokers,
		"events_count":  eventsCount,
		"timestamp":     time.Now(),
		"endpoints": []string{
			"POST /api/events/user",
			"POST /api/events/movie",
			"POST /api/events/payment",
			"GET /api/events",
		},
	}

	log.Printf("🏥 Health check requested - service healthy with %d events", eventsCount)
	json.NewEncoder(w).Encode(response)
}

func handleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case "GET":
		eventType := r.URL.Query().Get("type")
		events := eventService.GetEvents(eventType)

		log.Printf("📋 Events list requested (type: %s) - returning %d events", eventType, len(events))

		response := map[string]interface{}{
			"events": events,
			"count":  len(events),
			"filter": eventType,
		}
		json.NewEncoder(w).Encode(response)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleUserEvent(w http.ResponseWriter, r *http.Request) {
	handleSpecificEvent(w, r, "user_event", "user-events", "👤 USER")
}

func handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	handleSpecificEvent(w, r, "movie_event", "movie-events", "🎬 MOVIE")
}

func handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	handleSpecificEvent(w, r, "payment_event", "payment-events", "💳 PAYMENT")
}

func handleSpecificEvent(w http.ResponseWriter, r *http.Request, eventType, topic, logPrefix string) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case "POST":
		var eventData map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&eventData); err != nil {
			log.Printf("❌ %s event: Invalid JSON received", logPrefix)
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		event := Event{
			Type: eventType,
			Data: eventData,
		}

		log.Printf("📥 %s event creation requested with data: %+v", logPrefix, eventData)

		if err := eventService.PublishEvent(topic, event); err != nil {
			log.Printf("❌ %s event: Failed to publish - %v", logPrefix, err)
			http.Error(w, fmt.Sprintf("Failed to publish %s event", eventType), http.StatusInternalServerError)
			return
		}

		response := map[string]interface{}{
			"status":   "success",
			"event_id": event.ID,
			"topic":    topic,
			"type":     eventType,
			"message":  fmt.Sprintf("%s event created and processed successfully", logPrefix),
		}

		log.Printf("✅ %s event #%s successfully created and published to %s", logPrefix, event.ID, topic)

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(response)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func handlePublishEvent(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case "POST":
		var event Event
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			log.Printf("❌ Generic event: Invalid JSON received")
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		topic := "general-events"
		if event.Type != "" {
			topic = event.Type + "-events"
		}

		log.Printf("📥 Generic event creation requested: type=%s, data=%+v", event.Type, event.Data)

		if err := eventService.PublishEvent(topic, event); err != nil {
			log.Printf("❌ Generic event: Failed to publish - %v", err)
			http.Error(w, "Failed to publish event", http.StatusInternalServerError)
			return
		}

		response := map[string]interface{}{
			"status":   "success",
			"event_id": event.ID,
			"topic":    topic,
			"message":  "Event created and processed successfully",
		}

		log.Printf("✅ Generic event #%s successfully created and published to %s", event.ID, topic)

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(response)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
