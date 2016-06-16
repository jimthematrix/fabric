package async

import (
	"fmt"
	"time"

	gkc "github.com/elodina/go_kafka_client"
	"github.com/spf13/viper"
)

const FLAG_ZK = "kafka-zookeeper"
const FLAG_TOPIC = "kafka-topic-async-api"

type KafkaConnector struct {
}

func (c *KafkaConnector) SystemName() string {
	return "kafka"
}

func (c *KafkaConnector) RuntimeFlags() [][]string {
	return [][]string{{FLAG_ZK, "Addresses of Kafka zookeeper ensemble"}, {FLAG_TOPIC, "Kafka topic used to asynchronously submit transactions"}}
}

func (c *KafkaConnector) Start() error {
	zk := viper.GetString(FLAG_ZK)
	topic := viper.GetString(FLAG_TOPIC)

	config := getConsumerConfig(zk)
	//	config.Strategy = newPartitionTrackingStrategy(consumeStatus, -1)
	consumer := gkc.NewConsumer(config)
	consumer.StartStatic(map[string]int{topic: 2})

	connectorLogger.Info("------------- Kafka connector for async transaction API initialized ---------------\n")
	return nil
}

func (c *KafkaConnector) Close() error {
	return nil
}

func init() {
	ExternalConnectors.AddConnectorImpl(new(KafkaConnector))
}

func getConsumerConfig(zk string) *gkc.ConsumerConfig {
	config := gkc.DefaultConsumerConfig()
	config.AutoOffsetReset = gkc.SmallestOffset
	config.WorkerFailureCallback = func(_ *gkc.WorkerManager) gkc.FailedDecision {
		return gkc.CommitOffsetAndContinue
	}
	config.WorkerFailedAttemptCallback = func(_ *gkc.Task, _ gkc.WorkerResult) gkc.FailedDecision {
		return gkc.CommitOffsetAndContinue
	}
	config.Strategy = func(_ *gkc.Worker, msg *gkc.Message, id gkc.TaskId) gkc.WorkerResult {
		fmt.Printf("Message received\n\n%v\n\n", string(msg.Value))

		ProcessChaincode(msg.Value)

		return gkc.NewSuccessfulResult(id)
	}

	zkConfig := gkc.NewZookeeperConfig()
	zkConfig.ZookeeperConnect = []string{zk}
	zkConfig.MaxRequestRetries = 10
	zkConfig.ZookeeperSessionTimeout = 30 * time.Second
	zkConfig.RequestBackoff = 3 * time.Second
	config.Coordinator = gkc.NewZookeeperCoordinator(zkConfig)

	return config
}
