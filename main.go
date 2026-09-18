package main

import (
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/joho/godotenv"
	"kafkasaramaproducer/internal/producer"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file, using environment variables")
	}

	cfg := producer.Config{
		Brokers:  strings.Split(mustEnv("KAFKA_BROKERS"), ","),
		User:     mustEnv("KAFKA_USER"),
		Password: mustEnv("KAFKA_PASSWORD"),
		Topic:    mustEnv("KAFKA_TOPIC"),
	}
	// create producer
	p, err := producer.New(cfg)

	if err != nil {
		log.Fatalf("Error creating the sync producer: %v", err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			log.Printf("failed to close producer: %v", err)
		}
	}()

	count := mustEnvInt("KAFKA_MESSAGES_COUNT", 30)
	if count <= 0 {
		log.Fatalf("KAFKA_MESSAGES_COUNT must be positive, got %d", count)
	}

	// отправляем в несколько потоков
	// KAFKA_WORKERS - кол-во горутин
	workers := mustEnvInt("KAFKA_WORKERS", 10)

	jobs := make(chan int)
	var wg sync.WaitGroup

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for index := range jobs {
				producer.SendMessage(p, cfg.Topic, index)
			}
		}()
	}
	for i := 0; i < count; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	log.Println("Все сообщения отправлены")
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required env variable %s is empty", key)
	}
	return v
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustEnvInt(key string, def int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return def
	}
	result, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", key, value, err)
	}
	return result
}
