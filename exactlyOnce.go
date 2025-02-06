package main

import (
	"log"

	"github.com/IBM/sarama"
)

func main() {
	// Sarama config properties
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll // acks=all
	cfg.Producer.Return.Successes = true
	cfg.Producer.Idempotent = true // exactly-once
	cfg.Net.MaxOpenRequests = 1    //
	cfg.Producer.Retry.Max = 3
	cfg.Producer.Transaction.ID = "local-test-id"

	// Create producer
	producer, err := sarama.NewSyncProducer([]string{"kafka.sandbox.tutu.ru:9092"}, cfg)
	if err != nil {
		log.Fatalf("Error creating the sync producer: %v", err)
	}
	defer producer.Close()

	// begin transaction
	err = producer.BeginTxn()
	if err != nil {
		log.Fatalf("Error beginning a transaction: %v", err)
	}

	// test messages
	topic := "exactly-once-topic"
	key := sarama.StringEncoder("exactly-once-key")
	value := sarama.StringEncoder("exactly-once-value")

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   key,
		Value: value,
	}
	if err := producer.SendMessages([]*sarama.ProducerMessage{msg}); err != nil {
		log.Fatalf("Error sending message: %v", err)
		producer.AbortTxn()
		log.Fatalf("Transaction aborted: %v", err)
	}

	if err := producer.CommitTxn(); err != nil {
		log.Fatalf("Error committing transaction: %v", err)
		producer.AbortTxn()
		log.Fatalf("Transaction aborted: %v", err)
	}

	log.Printf("Transaction committed with exactly-once")

}
