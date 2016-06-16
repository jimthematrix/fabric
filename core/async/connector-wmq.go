package async

import (
	"errors"
	"time"

	"github.com/hyperledger/fabric/core/async/lib"

	"github.com/spf13/viper"
)

const FLAG_QMGR = "queue-manager"
const FLAG_QUEUE = "queue"

type WMQConnector struct {
	activeQueue bool
	queue       lib.MQQueue
	done        chan bool
}

func (c *WMQConnector) SystemName() string {
	return "wmq"
}

func (c *WMQConnector) RuntimeFlags() [][]string {
	return [][]string{{FLAG_QMGR, "Queue Manager name for the target queue in WebSphere MQ"}, {FLAG_QUEUE, "Target Queue name to pull transactions submission messages from in WebSphere MQ"}}
}

func (c *WMQConnector) Start() error {
	qmgrName := viper.GetString(FLAG_QMGR)
	qName := viper.GetString(FLAG_QUEUE)

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
				c.activeQueue = true
				c.queue = queue

				go c.poll()
			}
		}

		connectorLogger.Info("-------- WMQ connector initialized --------------\n")
		return nil
	} else {
		return errors.New("WebSphere MQ connector could not be successfully initialized due to missing configuration parameters: queue-manager")
	}
}

func (c *WMQConnector) Close() error {
	if err := c.queue.Close(); err != nil {
		return err
	}

	c.done <- true

	return nil
}

func init() {
	ExternalConnectors.AddConnectorImpl(new(WMQConnector))
}

func (c *WMQConnector) poll() {
	c.done = make(chan bool)
	tick := time.Tick(1 * time.Second)
	// Keep trying until we're timed out or got a result or got an error
	for {
		select {
		// Got a timeout! fail with a timeout error
		case <-c.done:
			return
		// Got a tick, we should check on doSomething()
		case <-tick:
			haveMessage, message, err := c.queue.Get()
			if err != nil {
				connectorLogger.Errorf("Failed to pull message from MQ queue: %v", err)
			}

			if haveMessage {
				connectorLogger.Infof("Received message: %v", message)
				ProcessChaincode([]byte(message))
			}
		}
	}
}
