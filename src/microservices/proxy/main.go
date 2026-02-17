package proxy

import (
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func main() {
	log.Println("Read environment")
	rand.Seed(time.Now().UnixNano())

	monolithURL := os.Getenv("MONOLITH_URL")
	moviesServiceURL := os.Getenv("MOVIES_SERVICE_URL")
	eventsServiceURL := os.Getenv("EVENTS_SERVICE_URL")
	gradualMigrationToggle := os.Getenv("GRADUAL_MIGRATION")
	gradualMigrationPercent := os.Getenv("MOVIES_MIGRATION_PERCENT")
	migrationEnabled := gradualMigrationToggle == "true"
	migrationPercent, err := strconv.Atoi(gradualMigrationPercent)
	if err != nil || migrationPercent < 0 || migrationPercent > 100 {
		migrationPercent = 0
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	addr := ":" + port

	log.Println("Environment readed")

	router := gin.Default()

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	router.Any("/api/movies/*path", func(c *gin.Context) {
		target := chooseMoviesTarget(
			monolithURL,
			moviesServiceURL,
			migrationEnabled,
			migrationPercent,
		)
		log.Printf(
			"Routing %s %s to %s",
			c.Request.Method,
			c.Request.RequestURI,
			target,
		)
		request(c, target)
	})

	router.Any("/api/events/*path", func(c *gin.Context) {
		request(c, eventsServiceURL)
	})

	router.Any("/api/*path", func(c *gin.Context) {
		request(c, monolithURL)
	})

	log.Printf("Proxy service running on %s", addr)
	router.Run(addr)
}

func request(c *gin.Context, targetBaseURL string) {
	targetURL := targetBaseURL + c.Request.RequestURI

	req, err := http.NewRequestWithContext(
		c.Request.Context(),
		c.Request.Method,
		targetURL,
		c.Request.Body,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	req.Header = c.Request.Header
	req.Host = c.Request.Host

	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	for k, v := range resp.Header {
		c.Writer.Header()[k] = v
	}

	c.Status(resp.StatusCode)
	c.Writer.Write(body)
}

func chooseMoviesTarget(
	monolithURL string,
	moviesURL string,
	migrationEnabled bool,
	migrationPercent int,
) string {
	if !migrationEnabled {
		return monolithURL
	}

	r := rand.Intn(100)
	if r < migrationPercent {
		return moviesURL
	}

	return monolithURL
}
