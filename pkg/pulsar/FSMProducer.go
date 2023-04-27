package pulsar

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/apache/pulsar-client-go/pulsar"
	"time"
)

type State string

const (
	Running State = "Running"
	Idle    State = "Idle"
	Error   State = "Error"
)

type Message struct {
	Message *pulsar.ProducerMessage
	Sender  chan string
}

type FSMProducer struct {
	client   pulsar.Client
	consumer *CompleteConsumer
	topic    string
	producer pulsar.Producer
	state    State
	buffer   chan *Message
}

func NewFSMProducer(client pulsar.Client, topic string, consumer *CompleteConsumer) *FSMProducer {
	producer, _ := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           topic,
		CompressionType: pulsar.LZ4,
	})
	fsm := &FSMProducer{
		client:   client,
		consumer: consumer,
		topic:    topic,
		producer: producer,
		buffer:   make(chan *Message, 1000),
		state:    Running,
	}
	go fsm.watch()
	return fsm
}

func (f *FSMProducer) closeProducer() {
	f.producer.Close()
	f.producer = nil
}

func (f *FSMProducer) recreateProducer() error {
	if f.producer != nil {
		f.producer.Close()
	}
	producer, err := f.client.CreateProducer(pulsar.ProducerOptions{
		Topic:           f.topic,
		CompressionType: pulsar.LZ4,
	})
	if err != nil {
		return err
	}
	f.producer = producer
	return nil
}

func (f *FSMProducer) watch() {
	for {
		if f.state == Running {
			select {
			case msg := <-f.buffer:
				f.handleMessage(msg)
			case <-time.After(5 * time.Second):
				f.closeProducer()
				f.goTo(Idle)
			}
		} else {
			msg := <-f.buffer
			err := f.recreateProducer()
			if err != nil {
				f.goTo(Error)
				f.handleError(msg.Sender, err)
			}
			f.goTo(Running)
			f.handleMessage(msg)
		}
	}
}

func (f *FSMProducer) handleMessage(msg *Message) {
	res, err := f.producer.Send(context.Background(), msg.Message)
	if err != nil {
		f.closeProducer()
		f.goTo(Error)
		f.handleError(msg.Sender, err)
	}
	msgId := base64.StdEncoding.EncodeToString(res.Serialize())
	f.consumer.RegisterListener(msgId, msg.Sender)
}

func (f *FSMProducer) handleError(sender chan<- string, err error) {
	data := make(map[string]interface{}, 1)
	data["error"] = err.Error()
	res, _ := json.Marshal(data)
	sender <- string(res)
}

func (f *FSMProducer) Send(msg *Message) {
	f.buffer <- msg
}

func (f *FSMProducer) goTo(state State) {
	f.state = state
}
