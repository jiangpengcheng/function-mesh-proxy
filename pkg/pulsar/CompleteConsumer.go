package pulsar

import "github.com/apache/pulsar-client-go/pulsar"

const CompletedTopic = "persistent://public/default/functions-exported-completed"
const MsgIdProperty = "__pfn_input_msg_id__"

type CompleteConsumer struct {
	consumer       pulsar.Consumer
	messageChannel chan pulsar.ConsumerMessage
	listeners      map[string]chan string
}

func NewCompleteConsumer(client pulsar.Client) (*CompleteConsumer, error) {
	channel := make(chan pulsar.ConsumerMessage, 100)
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            CompletedTopic,
		SubscriptionName: "asd",
		Type:             pulsar.Exclusive,
		MessageChannel:   channel,
	})
	if err != nil {
		return nil, err
	}
	res := &CompleteConsumer{
		consumer:       consumer,
		messageChannel: channel,
		listeners:      make(map[string]chan string),
	}
	go res.watch()
	return res, nil
}

func (c *CompleteConsumer) watch() {
	for cm := range c.messageChannel {
		if _, ok := cm.Message.Properties()[MsgIdProperty]; ok {
			if listener, exist := c.listeners[cm.Message.Properties()[MsgIdProperty]]; exist {
				listener <- string(cm.Message.Payload())
				c.listeners[cm.Message.Properties()[MsgIdProperty]] = nil
			}
		}
		cm.Consumer.Ack(cm.Message)
	}
}

func (c *CompleteConsumer) RegisterListener(id string, listener chan string) {
	c.listeners[id] = listener
}
