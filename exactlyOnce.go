package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"kafka.sandbox.tutu.ru:9092"}
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Error getting hostname: %v", err)
	}

	// Sarama config properties
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_8_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll // acks=all
	cfg.Producer.Return.Successes = true
	cfg.Producer.Idempotent = true // exactly-once
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Transaction.Retry.Backoff = 500 * time.Millisecond
	cfg.Producer.Transaction.ID = fmt.Sprintf("transactionApp-%s", hostname)

	// Create producer
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("Error creating the sync producer: %v", err)
	}
	defer producer.Close()

	// begin transaction
	if err := producer.BeginTxn(); err != nil {
		log.Fatalf("Error starting producer transaction: %v", err)
	}

	topic := "exactly-once-topic"
	messages := make([]*sarama.ProducerMessage, 0, 30)

	for i := 0; i < 30; i++ {
		currentTime := time.Now().Format("2006-01-02 15:04:05.000000")
		msg := fmt.Sprintf("Message #%d - Sent at %s", i, currentTime) // message
		producerMessage := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(msg),
		}
		messages = append(messages, producerMessage)
	}
	if err := producer.SendMessages(messages); err != nil {
		log.Printf("Error sending messages: %v", err)
		if abortErr := producer.AbortTxn(); abortErr != nil {
			log.Fatalf("Unsucsessfully aborting transaction: %v", abortErr)
		}
		return
	}

	// end transaction
	if err := producer.CommitTxn(); err != nil {
		log.Fatalf("Error committing transaction: %v", err)
	}

	log.Printf("Successfully committed %d messages", len(messages))
}
