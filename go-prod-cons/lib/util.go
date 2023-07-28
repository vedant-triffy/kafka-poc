package lib

import (
    "bufio"
    "fmt"
    "os"
    "strings"

    "github.com/confluentinc/confluent-kafka-go/kafka"
)

type Message struct {
    Key string
    Data string
}

func GetProducer(conf *kafka.ConfigMap) (*kafka.Producer, error) {
    return kafka.NewProducer(conf)
}

func CreateMessage(topic string, msg Message) kafka.Message {
    return kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
        Key:            []byte(msg.Key),
        Value:          []byte(msg.Data),
    }
}

func GetTopicName() string {
    return "triffy-ondc"
}

func ReadConfig(configFile string) kafka.ConfigMap {

    m := make(map[string]kafka.ConfigValue)

    file, err := os.Open(configFile)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed to open file: %s", err)
        os.Exit(1)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        line := strings.TrimSpace(scanner.Text())
        if !strings.HasPrefix(line, "#") && len(line) != 0 {
            kv := strings.Split(line, "=")
            parameter := strings.TrimSpace(kv[0])
            value := strings.TrimSpace(kv[1])
            m[parameter] = value
        }
    }

    if err := scanner.Err(); err != nil {
        fmt.Printf("Failed to read file: %s", err)
        os.Exit(1)
    }

    return m

}
