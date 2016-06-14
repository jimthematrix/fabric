package producer

import (
	"fmt"
	"errors"

	pb "github.com/hyperledger/fabric/protos"

	"github.com/spf13/viper"
	"github.com/hyperledger/fabric/events/producer/lib"
)

type WMQConnector struct {
	Queue lib.MQQueue
	ActiveQueue bool
}

func init() {
	ExternalConnectors.AddConnectorImpl(new(WMQConnector))
}

func (c *WMQConnector) SystemName() string {
	return "wmq"
}

func (c *WMQConnector) Initialize() error {

	qmgrName := viper.GetString("queue-manager")
	qName := viper.GetString("queue")	
	
	var qManager lib.MQ
	var queue lib.MQQueue
	var err2 error

	if len(qmgrName) > 0 {
		err1 := qManager.Connect(qmgrName)

		if err1 != nil {
			connectorLogger.Error("Failed to connect to MQ queue manager. %v\n", err1)
			return err1
		} else {
			connectorLogger.Info("Connection to WebSphere MQ successfully established\n")

			queue, err2 = qManager.Open(qName)
			if err2 != nil {
				connectorLogger.Error("Failed to open queue %v. %v\n", qName, err2)
				return err2
			} else {
				c.ActiveQueue = true
				c.Queue = queue
			}
		}

		connectorLogger.Info("-------- WMQ connector initialized --------------\n")
		return nil
	} else {
		return errors.New("WebSphere MQ connector could not be successfully initialized due to missing configuration parameters: queue-manager")
	}
}

func (c *WMQConnector) Publish(msg *pb.Event) error {
	if c.ActiveQueue {
		err := c.Queue.Put(fmt.Sprintf("Transaction result: %v", msg))

		if err != nil {
			connectorLogger.Error("Failed to produce message to WebSphere MQ. %v", err)
			return err
		}

		connectorLogger.Info("------------- Event published to WebSphere MQ ---------------\n")
	}

	return nil
}

func (c *WMQConnector) Close() error {
    if err := c.Queue.Close(); err != nil {
        return err
    }

    return nil
}