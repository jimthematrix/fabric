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
	"os/signal"
	"log"

	"github.com/hyperledger/fabric/events/consumer"
	pb "github.com/hyperledger/fabric/protos"
	"github.com/Shopify/sarama"
)

type adapter struct {
	notfy chan *pb.Event_Block
}

//GetInterestedEvents implements consumer.EventAdapter interface for registering interested events
func (a *adapter) GetInterestedEvents() ([]*pb.Interest, error) {
	return []*pb.Interest{&pb.Interest{EventType: "block", ResponseType: pb.Interest_PROTOBUF}}, nil
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
	var eventAddress string
	flag.StringVar(&eventAddress, "events-address", "0.0.0.0:31315", "address of events server")
	flag.Parse()

	fmt.Printf("Event Address: %s\n", eventAddress)

	a := createEventClient(eventAddress)
	if a == nil {
		fmt.Printf("Error creating event client\n")
		return
	}

	producer, err := sarama.NewAsyncProducer([]string{"192.168.99.100:9092"}, nil)
	if err != nil {
	    panic(err)
	}

	defer func() {
	    if err := producer.Close(); err != nil {
	        log.Fatalln(err)
	    }
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enqueued, errors int
	var message sarama.StringEncoder

	ProducerLoop:
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
					message = sarama.StringEncoder(r.Error)
				} else {
					fmt.Printf("Success Transaction:\n\t[%v]\n", r)
					message = sarama.StringEncoder(r.Uuid)
				}

			    select {
			    case producer.Input() <- &sarama.ProducerMessage{Topic: "hl", Key: nil, Value: message}:
			        enqueued++
			    case err := <-producer.Errors():
			        log.Println("Failed to produce message", err)
			        errors++
			    case <-signals:
			        break ProducerLoop
			    }
			}
		}
	}

	log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}
