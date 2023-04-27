package main

import (
	"flag"
	"github.com/apache/pulsar-client-go/pulsar"
	pulsar2 "github.com/streamnative/function-mesh-proxy/pkg/pulsar"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type createProducerMessage struct {
	topic  string
	sender chan bool
}

func getIntermediateTopic(tenant, namespace, name string) string {
	return "persistent://" + tenant + "/" + namespace + "/" + name + "-exported-intermediate"
}

func setupRouter(client pulsar.Client) *gin.Engine {
	unifiedConsumer, err := pulsar2.NewCompleteConsumer(client)
	if err != nil {
		panic(err)
	}
	cachedProducers := make(map[string]*pulsar2.FSMProducer, 0)
	createSignal := make(chan createProducerMessage, 100)
	go func() {
		for message := range createSignal {
			if _, ok := cachedProducers[message.topic]; !ok {
				producer := pulsar2.NewFSMProducer(client, message.topic, unifiedConsumer)
				cachedProducers[message.topic] = producer
			}
			message.sender <- true // producer is just created or already exists
		}
	}()

	r := gin.Default()

	// Ping test
	r.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK, "pong")
	})

	r.POST("/:tenant/:namespace/:name", func(c *gin.Context) {
		tenant := c.Param("tenant")
		namespace := c.Param("namespace")
		name := c.Param("name")
		jsonData, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read request body"})
			return
		}
		producer, ok := cachedProducers[tenant+"/"+namespace+"/"+name]
		if !ok {
			sender := make(chan bool, 1)
			createSignal <- createProducerMessage{topic: getIntermediateTopic(tenant, namespace, name), sender: sender}
			created := <-sender // wait producer to be created
			if !created {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create producer"})
				return
			}
			producer = cachedProducers[tenant+"/"+namespace+"/"+name]
		}
		sender := make(chan string, 1)
		producer.Send(&pulsar2.Message{
			Message: &pulsar.ProducerMessage{
				Value: jsonData,
			},
			Sender: sender,
		})
		select {
		case result := <-sender:
			c.JSON(http.StatusOK, result)
		case <-time.After(60 * time.Second):
			c.JSON(http.StatusInternalServerError, gin.H{"error": "timeout to wait for response"})
		}
	})

	return r
}

func main() {
	var listenAddr string
	var pulsarURL string
	flag.StringVar(&listenAddr, "listen-addr", ":8080", "listen address")
	flag.StringVar(&pulsarURL, "pulsar-url", "pulsar://localhost:6650", "pulsar url")

	conf := pulsar.ClientOptions{URL: pulsarURL}
	client, _ := pulsar.NewClient(conf)
	defer client.Close()

	r := setupRouter(client)
	// Listen and Server in 0.0.0.0:8080
	err := r.Run(listenAddr)
	if err != nil {
		panic(err)
	}
}
