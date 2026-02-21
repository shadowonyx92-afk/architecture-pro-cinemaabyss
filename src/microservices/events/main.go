package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

var (
	kafkaURL   string
	kafkaTopic string = "movies-events"
)

func main() {
	kafkaURL = os.Getenv("KAFKA_BROKERS")
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	addr := ":" + port

	router := gin.Default()

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": true})
	})

	router.GET("/api/events/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": true})
	})

	router.POST("/api/events/movie", func(c *gin.Context) {
		produce("Movie", c)
	})

	router.POST("/api/events/user", func(c *gin.Context) {
		produce("User", c)
	})

	router.POST("/api/events/payment", func(c *gin.Context) {
		produce("Payment", c)
	})

	router.POST("/api/events", func(c *gin.Context) {
		eventType := c.Query("type") // User, Payment, Movie
		if eventType == "" {
			eventType = "Common"
		}

		err := produceEvent(eventType)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "success", "type": eventType})
	})

	go consumeEvents()

	log.Printf("Events service running on %s", addr)
	router.Run(addr)
}

func produce(eventType string, c *gin.Context) {
	if err := produceEvent(eventType); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"status": "success", "type": eventType})
}

func produceEvent(eventType string) error {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaURL},
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	msg := kafka.Message{
		Key:   []byte(time.Now().Format(time.RFC3339Nano)),
		Value: []byte("Event type: " + eventType),
	}

	return writer.WriteMessages(context.Background(), msg)
}

func consumeEvents() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		Topic:    kafkaTopic,
		GroupID:  "events-service-group",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("Error reading message:", err)
			continue
		}
		log.Printf("Consumed message: %s\n", string(m.Value))
	}
}
