package connectors

import (
	"fmt"
	"errors"

	pb "github.com/hyperledger/fabric/protos"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
	"github.com/Shopify/sarama"
)

var producerLogger = logging.MustGetLogger("Kafka-connector")

type KafkaConnector struct {
	Producer sarama.AsyncProducer
}

func (c *KafkaConnector) SystemName() string {
	return "kafka"
}

func (c *KafkaConnector) Initialize() error {
	kafkaBrokers := viper.GetString("kafka-brokers")

	if len(kafkaBrokers) > 0 {
		producerLogger.Info("Kafka broker list: %s\n", kafkaBrokers)

		var err error
		c.Producer, err = sarama.NewAsyncProducer([]string{kafkaBrokers}, nil)

		if err != nil {
		    return err
		}

		producerLogger.Info("------------- Kafka connector initialized ---------------\n")
		return nil
	} else {
		return errors.New("Kafka connector could not be successfully initialized due to missing configurations: kafka-brokers")
	}
}

func (c *KafkaConnector) Publish(msg *pb.Event) error {
	kafkaTopic := viper.GetString("kafka-topic")

	if len(kafkaTopic) > 0 {
	    select {
	    case c.Producer.Input() <- &sarama.ProducerMessage{Topic: kafkaTopic, Key: nil, Value: sarama.StringEncoder(fmt.Sprintf("%v", msg))}:
			producerLogger.Info("------------- Event published to Kafka ---------------\n")
			return nil
	    case err := <-c.Producer.Errors():
	        producerLogger.Error("Failed to produce message", err)
	        return err
	    }
	} else {
		return errors.New("Kafka connector could not be successfully initialized due to missing configurations: kafka-brokers")
	}
}

func (c *KafkaConnector) Close() error {
    if err := c.Producer.Close(); err != nil {
        return err
    }

    return nil
}