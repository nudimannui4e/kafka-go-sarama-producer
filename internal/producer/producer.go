package producer

import (
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	scram_client "kafkasaramaproducer/internal/scram"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type Config struct {
	Brokers  []string
	User     string
	Password string
	Topic    string
}

func New(cfg Config) (sarama.SyncProducer, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V3_8_0_0
	saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Net.MaxOpenRequests = 1
	saramaCfg.Producer.Retry.Max = 5
	saramaCfg.Producer.Retry.Backoff = 1 * time.Second

	saramaCfg.Net.SASL.Enable = true
	saramaCfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	saramaCfg.Net.SASL.User = cfg.User
	saramaCfg.Net.SASL.Password = cfg.Password
	saramaCfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return &scram_client.XDGSCRAMClient{HashGeneratorFcn: sha256.New}
	}

	saramaCfg.Net.TLS.Enable = true
	saramaCfg.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}

	return sarama.NewSyncProducer(cfg.Brokers, saramaCfg)
}

// SendMessage отправляет одно сообщение с ретраями.
func SendMessage(p sarama.SyncProducer, topic string, index int) {
	for attempt := 0; attempt < 5; attempt++ {
		currentTime := time.Now().Format("2006-01-02 15:04:05.00000")
		text := fmt.Sprintf("Message #%d - Sent at %s", index, currentTime)

		partition, offset, err := p.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(text),
		})
		if err != nil {
			log.Printf("Попытка #%d: ошибка отправки сообщения #%d: %v", attempt+1, index, err)
			time.Sleep(time.Second)
			continue
		}
		log.Printf("Сообщение #%d отправлено в партицию %d, offset %d", index, partition, offset)
		return
	}
	log.Printf("Не удалось отправить сообщение #%d после 5 попыток", index)
}
