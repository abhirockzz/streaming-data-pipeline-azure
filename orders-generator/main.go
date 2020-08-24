package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-uuid"
)

const eventHubsConnStringEnvVar = "EVENTHUBS_CONNECTION_STRING"
const eventHubsBrokerEnvVar = "EVENTHUBS_BROKER"
const eventHubsTopicEnvVar = "EVENTHUBS_TOPIC"

var producer sarama.SyncProducer

func init() {
	brokerList := []string{os.Getenv(eventHubsBrokerEnvVar)}
	fmt.Println("connecting to Kafka", brokerList)

	var err error

	producer, err = sarama.NewSyncProducer(brokerList, getAuthConfig())
	if err != nil {
		log.Fatalf("failed to create kafka producer %v", err)
	}

	log.Println("kafka producer created")
}

func getAuthConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Net.DialTimeout = 10 * time.Second

	config.Net.SASL.Enable = true
	config.Net.SASL.User = "$ConnectionString"
	config.Net.SASL.Password = os.Getenv(eventHubsConnStringEnvVar)
	config.Net.SASL.Mechanism = "PLAIN"

	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tls.Config{
		InsecureSkipVerify: true,
		ClientAuth:         0,
	}
	config.Version = sarama.V1_0_0_0
	config.Producer.Return.Successes = true
	return config
}

func main() {
	defer func() {
		err := producer.Close()
		if err != nil {
			log.Println("error closing kafka producer", err)
			return
		}
		log.Println("closed producer")
	}()

	eventHubsTopic := os.Getenv(eventHubsTopicEnvVar)
	fmt.Println("Event Hubs topic", eventHubsTopic)

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan bool)

	go func() {
		for {
			select {
			case <-exit:
				log.Println("exit signalled")
				done <- true
				return
			default:
				rCustID := rand.Intn(10000) + 1 //1 to 10000
				rOrderID, _ := uuid.GenerateUUID()
				rAmt := rand.Intn(480) + 20

				rOrder, _ := json.Marshal(Order{rOrderID, rCustID, rAmt})

				msg := &sarama.ProducerMessage{Topic: eventHubsTopic, Key: sarama.StringEncoder(rOrderID), Value: sarama.ByteEncoder(rOrder)}
				p, o, err := producer.SendMessage(msg)
				if err != nil {
					fmt.Println("Failed to send msg:", err)
				}
				fmt.Printf("sent message to partition %d offset %d\n%s\n", p, o, string(rOrder))

				time.Sleep(3 * time.Second) //intentional pause
			}
		}
	}()

	<-done
	log.Println("program closed")
}

type Order struct {
	OrderID    string `json:"orderID"`
	CustomerID int    `json:"customerID"`
	Amount     int    `json:"amount"`
}
