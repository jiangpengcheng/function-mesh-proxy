package pulsar

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"time"
)

type State string

const (
	Running State = "Running"
	Idle    State = "Idle"
	Error   State = "Error"
)

type MessageIdData struct {
	LedgerId   *uint64 `protobuf:"varint,1,req,name=ledgerId" json:"ledgerId,omitempty"`
	EntryId    *uint64 `protobuf:"varint,2,req,name=entryId" json:"entryId,omitempty"`
	Partition  *int32  `protobuf:"varint,3,opt,name=partition,def=-1" json:"partition,omitempty"`
	BatchIndex *int32  `protobuf:"varint,4,opt,name=batch_index,json=batchIndex,def=-1" json:"batch_index,omitempty"`
	AckSet     []int64 `protobuf:"varint,5,rep,name=ack_set,json=ackSet" json:"ack_set,omitempty"`
	BatchSize  *int32  `protobuf:"varint,6,opt,name=batch_size,json=batchSize" json:"batch_size,omitempty"`
	// For the chunk message id, we need to specify the first chunk message id.
	FirstChunkMessageId  *MessageIdData `protobuf:"bytes,7,opt,name=first_chunk_message_id,json=firstChunkMessageId" json:"first_chunk_message_id,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *MessageIdData) Reset()         { *m = MessageIdData{} }
func (m *MessageIdData) String() string { return proto.CompactTextString(m) }
func (*MessageIdData) ProtoMessage()    {}

type Message struct {
	Error   error
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
				if msg.Error != nil {
					f.closeProducer()
					f.goTo(Error)
				} else {
					f.handleMessage(msg)
				}
			case <-time.After(5 * time.Second):
				f.closeProducer()
				f.goTo(Idle)
			}
		} else {
			msg := <-f.buffer
			if msg.Error != nil {
				f.goTo(Error)
			} else {
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
}

func (f *FSMProducer) handleMessage(msg *Message) {
	f.producer.SendAsync(context.Background(), msg.Message, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
		if err != nil {
			f.buffer <- &Message{Error: err}
			f.handleError(msg.Sender, err)
		}
		newId := &MessageIdData{
			LedgerId:   proto.Uint64(uint64(id.LedgerID())),
			EntryId:    proto.Uint64(uint64(id.EntryID())),
			BatchIndex: proto.Int32(id.BatchIdx()),
		}
		data, err := proto.Marshal(newId)
		if err != nil {
			f.handleError(msg.Sender, err)
		} else {
			msgId := base64.StdEncoding.EncodeToString(data)
			f.consumer.RegisterListener(msgId, msg.Sender)
		}
	})
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
