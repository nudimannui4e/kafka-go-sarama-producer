package main

import (
	"log"
	"fmt"
	"time"
	"sync"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"kafka.sandbox.tutu.ru:9092"}

	// Sarama config properties
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_8_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll // acks=all
	cfg.Producer.Return.Successes = true
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 1 * time.Second // 1sec timeout between retry


	// Create producer
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("Error creating the sync producer: %v", err)
	}
	defer producer.Close()

	topic := "exactly-once-topic"

	var wg sync.WaitGroup

	for i := 0; i < 30; i++ {
		wg.Add(1) // Увеличиваем счётчик goroutine
		go func(index int) {
			defer wg.Done() // Уменьшаем счётчик после завершения горутины
			sendMessage(producer, topic, index)
		}(i)
	}

	// Ожидаем завершения всех goroutines
	wg.Wait()
	log.Println("Все сообщения отправлены")
}

func sendMessage(producer sarama.SyncProducer, topicName string, index int) {
	for attempt := 0; attempt < 5; attempt++ {
	// Формируем сообщение
		currentTime := time.Now().Format("2006-01-02 15:04:05.00000")
		messageText := fmt.Sprintf("Message #%d - Sent at %s", index, currentTime)
		producerMessage := &sarama.ProducerMessage{
		  Topic: topicName,
		  Value: sarama.StringEncoder(messageText),
		}

		// Попытка отправить сообщение
		partition, offset, err := producer.SendMessage(producerMessage)
		if err != nil {
		  log.Printf("Попытка #%d: Ошибка отправки сообщения #%d: %v", attempt+1, index, err)
		  time.Sleep(time.Second) // Ожидание перед следующей попыткой
		} else {
		  // Успешная отправка
		  log.Printf("Сообщение #%d успешно отправлено в партицию %d с оффсетом %d", index, partition, offset)
		  return
		}
	}
	// Если все попытки не успешны
    log.Printf("Не удалось отправить сообщение #%d после 5 попыток", index)
}


