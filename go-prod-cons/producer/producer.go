package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"lib"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
)

type Payload struct {
	ID    string `json:"id"`
	Topic string `json:"topic"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type AckResp struct {
	Ack  string    `json:"ack"`
	Time time.Time `json:"time"`
}

func main() {

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <config-file-path>\n",
			os.Args[0])
		os.Exit(1)
	}
	configFile := os.Args[1]
	conf := lib.ReadConfig(configFile)

	topic := lib.GetTopicName()
	p, err := lib.GetProducer(&conf)

	if err != nil {
		fmt.Printf("Failed to create producer: %s", err)
		os.Exit(1)
	}

    // NOTE: for some reason removing this causes 15s latency per request - INVESTIGATE
	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	r := gin.Default()
	r.POST("/send-msg", func(ctx *gin.Context) {
        // Unmarshal request body
		body, _ := ioutil.ReadAll(ctx.Request.Body)
		var payload Payload
		json.Unmarshal(body, &payload)

        msg := lib.CreateMessage(topic, lib.Message{Key: payload.Key, Data: payload.Value})
        p.Produce(&msg, nil)

		// send ack response
		w := ctx.Writer
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(AckResp{Ack: "ok", Time: time.Now()})

		// wait for message to deliver
		p.Flush(15 * 1000)
	})

	r.Run()
}
