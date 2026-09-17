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
	defer p.Close()

	count, _ := strconv.Atoi(getEnv("KAFKA_MESSAGES_COUNT", "30"))

	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			producer.SendMessage(p, cfg.Topic, index)
		}(i)
	}
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
	if v := os.Getenv(key); v == "" {
		return v
	}
	return def
}
