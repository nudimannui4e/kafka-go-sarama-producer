package main

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

// Блок ниже, это копипаст примера
// https://github.com/IBM/sarama/blob/main/examples/sasl_scram_client/scram_client.go
// SHA256 и SHA512 — это функции-генераторы хешей
var SHA256 scram.HashGeneratorFcn = sha256.New
var SHA512 scram.HashGeneratorFcn = sha512.New

// XDGSCRAMClient — реализация интерфейса sarama.SCRAMClient
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

func main() {
	brokers := []string{"kafka-500.tutu-wallet.devel.tutu.ru:9093"}

	// Sarama config properties
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_8_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll // acks=all
	cfg.Producer.Return.Successes = true
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 1 * time.Second // 1sec timeout between retry
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	cfg.Net.SASL.User = "admin-broker"
	cfg.Net.SASL.Password = "D4pmTidapkKJsWOp"
	cfg.Net.TLS.Enable = true
	cfg.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}
	cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return &XDGSCRAMClient{HashGeneratorFcn: sha256.New}
	}
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
