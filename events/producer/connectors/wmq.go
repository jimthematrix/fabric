package connectors

import (
	"fmt"

	pb "github.com/hyperledger/fabric/protos"
	// "github.com/Shopify/sarama"
)

type WMQConnector struct {
	
}

func (c *WMQConnector) SystemName() string {
	return "wmq"
}

func (c *WMQConnector) Initialize() error {

	// var producer sarama.AsyncProducer

	// kafkaBrokers := viper.GetString("kafka-brokers")
	// kafkaTopic := viper.GetString("kafka-topic")

	// if len(kafkaBrokers) > 0 {
	// 	fmt.Printf("Kafka broker list: %s\n", kafkaBrokers)

	// 	var err error
	// 	producer, err = sarama.NewAsyncProducer([]string{kafkaBrokers}, nil)
	// 	if err != nil {
	// 	    panic(err)
	// 	}

	// 	defer func() {
	// 	    if err := producer.Close(); err != nil {
	// 	        log.Fatalln(err)
	// 	    }
	// 	}()
	// }

	fmt.Printf("-------- WMQ connector initialized --------------\n")
	return nil
}

func (c *WMQConnector) Publish(msg *pb.Event) error {
	// if len(kafkaBrokers) > 0 {
	//     select {
	//     case producer.Input() <- &sarama.ProducerMessage{Topic: kafkaTopic, Key: nil, Value: sarama.StringEncoder("Successful transaction")}:
	//         enqueued++
	//     case err := <-producer.Errors():
	//         log.Println("Failed to produce message", err)
	//         errors++
	//     }
	// }

	fmt.Printf("Published\n")
	return nil
}

func (c *WMQConnector) Close() error {
	return nil
}