package stream_kafka

import (
	structs "github.com/antalakas/stream-structs"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/viper"
	log "github.com/sirupsen/logrus"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type cbfKafkaConsumer struct {
	KakfaConsumer *kafka.Consumer
	Topic string
	push_back time.Duration
}

func (consumer *cbfKafkaConsumer) Init(continent string) (string,
	*kafka.Consumer) {
	var kafkaConsumer = viper.GetStringMapString(
		fmt.Sprintf("%s%s%s",
			"source.kafka.", continent, ".consumer.bootstrap_servers"))

	kafkaConsumerBootstrapServers := make([]string, len(kafkaConsumer))
	kafkaConsumerBootstrapServersIndex := 0
	for _, v := range kafkaConsumer {
		kafkaConsumerBootstrapServers[kafkaConsumerBootstrapServersIndex] = v
		kafkaConsumerBootstrapServersIndex++
	}

	consumer.Topic = viper.GetString(fmt.Sprintf(
		"%s%s%s", "source.kafka.", continent, ".consumer.topic"))
	var group_id = viper.GetString(fmt.Sprintf(
		"%s%s%s", "source.kafka.", continent, ".consumer.group_id"))

	consumer.push_back = time.Duration(viper.GetInt(fmt.Sprintf(
		"%s%s%s", "source.kafka.",
		continent, ".consumer.push_back")))
	var session_timeout_ms = viper.GetInt(fmt.Sprintf(
		"%s%s%s", "source.kafka.", continent,
		".consumer.session_timeout_ms"))
	var go_application_rebalance_enable = viper.GetBool(fmt.Sprintf(
		"%s%s%s", "source.kafka.", continent,
		".consumer.go_application_rebalance_enable"))
	var auto_offset_reset = viper.GetString(fmt.Sprintf(
		"%s%s%s", "source.kafka.", continent,
		".consumer.auto_offset_reset"))
	var fetch_min_bytes = viper.GetInt(fmt.Sprintf(
		"%s%s%s", "source.kafka.", continent,
		".consumer.fetch_min_bytes"))
	var fetch_message_max_bytes = viper.GetInt(fmt.Sprintf(
		"%s%s%s", "source.kafka.", continent,
		".consumer.fetch_message_max_bytes"))
	var fetch_wait_max_ms = viper.GetInt(fmt.Sprintf(
		"%s%s%s", "source.kafka.", continent,
		".consumer.fetch_wait_max_ms"))

	bootstrap_servers :=
		strings.Trim(strings.Join(strings.Fields(fmt.Sprint(
			kafkaConsumerBootstrapServers)), ","), "[]")

	log.WithFields(log.Fields{
		"bootstrap_servers": bootstrap_servers,
		"topic": consumer.Topic,
		"group_id": group_id,
		"go_application_rebalance_enable": go_application_rebalance_enable,
		"auto_offset_reset":  auto_offset_reset,
		"fetch_min_bytes": fetch_min_bytes,
		"fetch_message_max_bytes": fetch_message_max_bytes,
		"fetch_wait_max_ms": fetch_wait_max_ms,
	}).Info("Kafka configuration: ")

	var err error
	consumer.KakfaConsumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrap_servers,
		"group.id":           group_id,
		"session.timeout.ms": session_timeout_ms,
		"go.events.channel.enable": true,
		"go.application.rebalance.enable": go_application_rebalance_enable,
		"auto.offset.reset":  auto_offset_reset,
		"enable.partition.eof": true,
		"fetch.min.bytes": fetch_min_bytes,
		"fetch.message.max.bytes": fetch_message_max_bytes,
		"fetch.wait.max.ms": fetch_wait_max_ms})

	if err != nil {
		log.Info(fmt.Sprintf("Failed to create consumer: %s\n", err))
		os.Exit(1)
	}

	return consumer.Topic, consumer.KakfaConsumer
}

func (consumer *cbfKafkaConsumer) ConsumeStream(
	empty structs.EmptyInterface) {


	log.Info("Created Consumer %v\n", consumer.KakfaConsumer)

	err := consumer.KakfaConsumer.SubscribeTopics(
		[]string{consumer.Topic}, nil)

	if err != nil {
		log.Info(fmt.Sprintf(
			"Unable to subscribe to topic: %s\n", err))
		os.Exit(1)
	}

	run := true

	counterThreshold := 10000.0
	counter := 1

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	rate := consumer.push_back * time.Microsecond
	throttle := time.Tick(rate)

	for run == true {
		select {
		case sig := <-sigchan:
			log.Info("Caught signal %v: terminating\n", sig)
			run = false

		case ev := <-consumer.KakfaConsumer.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				log.Info(fmt.Sprintf("%% %v\n", e))

				// cast to the proper type
				kafkaChannelWrapper, _ := empty.(structs.KafkaChannelWrapper)
				// now use the directional channel
				consumer.assignedPartitionsSender("assigned",
					kafkaChannelWrapper.AssignedPartitionsChannel)

				consumer.KakfaConsumer.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				//log.Info(fmt.Sprintf("%% %v\n", e))
				consumer.KakfaConsumer.Unassign()
			case *kafka.Message:
				//log.WithFields(log.Fields{
				//	"Topic": e.TopicPartition.Topic,
				//	"Partition": e.TopicPartition.Partition,
				//	"Offset": e.TopicPartition.Offset,
				//	"Key": e.Key,
				//	"Value": fmt.Sprintf("%s", e.Value),
				//}).Info("Message: ")

				<-throttle

				// cast to the proper type
				kafkaChannelWrapper, _ := empty.(structs.KafkaChannelWrapper)
				// now use the directional channel
				consumer.messageSender(e, kafkaChannelWrapper.KafkaChannel)

				if math.Mod(float64(counter), counterThreshold) == 0 {
					counter = 1
					log.Info("10000 incoming events...")
				} else {
					counter = counter + 1
				}

				break
			case kafka.PartitionEOF:
				log.Info(fmt.Sprintf("%% Reached %v\n", e))
			case kafka.Stats:
				log.Info(fmt.Sprintf("%% Stats: %v\n", e))
			case kafka.Error:
				// Errors should generally be considered as informational,
				// the client will try to automatically recover
				log.Info(fmt.Sprintf("%% Error: %v\n", e))
			default:
				log.Info(fmt.Sprintf("Ignored %v\n", e))
			}
		}
	}

	log.Info("Closing consumer\n")
	err = consumer.KakfaConsumer.Close()
	os.Exit(1)
}

// internal function to actually use the directional channel
func (consumer *cbfKafkaConsumer) messageSender(theMessage *kafka.Message,
	messageChan chan<- *kafka.Message) {
	messageChan <- theMessage
}

// internal function to actually use the directional channel
func (consumer *cbfKafkaConsumer) assignedPartitionsSender(
	assignedMessage string,
	assignedPartitionsChan chan<- string) {
	assignedPartitionsChan <- assignedMessage
}

var CBFKafkaConsumer cbfKafkaConsumer
