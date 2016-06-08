/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/hyperledger/fabric/events/consumer"
	pb "github.com/hyperledger/fabric/protos"
	"github.com/hyperledger/gomq/lib"
)

type adapter struct {
	notfy chan *pb.Event_Block
}

//GetInterestedEvents implements consumer.EventAdapter interface for registering interested events
func (a *adapter) GetInterestedEvents() ([]*pb.Interest, error) {
	return []*pb.Interest{{EventType: "block", ResponseType: pb.Interest_PROTOBUF}}, nil
}

//Recv implements consumer.EventAdapter interface for receiving events
func (a *adapter) Recv(msg *pb.Event) (bool, error) {
	switch msg.Event.(type) {
	case *pb.Event_Block:
		a.notfy <- msg.Event.(*pb.Event_Block)
		return true, nil
	default:
		a.notfy <- nil
		return false, nil
	}
}

//Disconnected implements consumer.EventAdapter interface for disconnecting
func (a *adapter) Disconnected(err error) {
	fmt.Printf("Disconnected...exiting\n")
	os.Exit(1)
}

func createEventClient(eventAddress string) *adapter {
	var obcEHClient *consumer.EventsClient

	done := make(chan *pb.Event_Block)
	adapter := &adapter{notfy: done}
	obcEHClient = consumer.NewEventsClient(eventAddress, adapter)
	if err := obcEHClient.Start(); err != nil {
		fmt.Printf("could not start chat %s\n", err)
		obcEHClient.Stop()
		return nil
	}

	return adapter
}

func main() {
	var eventAddress, qmgrName, qName string
	flag.StringVar(&eventAddress, "events-address", "0.0.0.0:31315", "address of events server")
	flag.StringVar(&qmgrName, "queue-manager", "", "Queue Manager name for the target queue in WebSphere MQ")
	flag.StringVar(&qName, "queue", "QUEUE1", "Target Queue name to put events in WebSphere MQ. Default: QUEUE1")
	flag.Parse()

	fmt.Printf("Event Address: %s\n", eventAddress)

	a := createEventClient(eventAddress)
	if a == nil {
		fmt.Printf("Error creating event client\n")
		return
	}

	var qManager lib.MQ
	var queue lib.MQQueue
	var activeQueue bool

	if len(qmgrName) > 0 {
		err := qManager.Connect(qmgrName)
		if err != nil {
			fmt.Printf("Failed to connect to MQ queue manager. %v\n", err)
		} else {
			fmt.Printf("Connection successfully established\n")

			queue, err = qManager.Open(qName)
			if err != nil {
				fmt.Printf("Failed to open queue %v\n", qName)
			} else {
				activeQueue = true
				defer queue.Close()
			}
		}
	}

	for {
		b := <-a.notfy
		if b.Block.NonHashData.TransactionResults == nil {
			fmt.Printf("INVALID BLOCK ... NO TRANSACTION RESULTS %v\n", b)
		} else {
			fmt.Printf("Received block\n")
			fmt.Printf("--------------\n")
			for _, r := range b.Block.NonHashData.TransactionResults {
				if r.ErrorCode != 0 {
					fmt.Printf("Err Transaction:\n\t[%v]\n", r)
				} else {
					fmt.Printf("Success Transaction:\n\t[%v]\n", r)
				}

				if activeQueue {
					queue.Put(fmt.Sprintf("Transaction result: %v", r))
				}
			}
		}
	}
}
