package main

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
	ex "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	ext "github.com/reugn/go-streams/kafka"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	hosts := []string{"10.19.8.243:9092"}
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Producer.Return.Successes = true
	config.Version, _ = sarama.ParseKafkaVersion("3.6.2")
	groupID := "testConsumer3"

	source, err := ext.NewKafkaSource(ctx, hosts, groupID, config, "test")
	if err != nil {
		log.Fatal(err)
	}

	toUpperMapFlow := flow.NewMap(toUpper, 1)
	appendAsteriskFlatMapFlow := flow.NewFlatMap(appendAsterisk, 1)
	//appendAsteriskFlatMapFlow := flow.NewMap(appendAsterisk, 1)
	//sink, err := ext.NewKafkaSink(hosts, config, "test2")
	stdoutSink := ex.NewStdoutSink()
	if err != nil {
		log.Fatal(err)
	}

	throttler := flow.NewThrottler(100, time.Second, 3*100, flow.Discard)
	tumblingWindow := flow.NewTumblingWindow[*sarama.ConsumerMessage](time.Second * 5)

	source.
		Via(toUpperMapFlow).
		Via(throttler).
		Via(tumblingWindow).
		Via(appendAsteriskFlatMapFlow).
		To(stdoutSink)
}

var toUpper = func(msg *sarama.ConsumerMessage) *sarama.ConsumerMessage {
	msg.Value = []byte(strings.ToUpper(string(msg.Value)))
	return msg
}

var appendAsterisk = func(inMessages []*sarama.ConsumerMessage) []*sarama.ConsumerMessage {
	outMessages := make([]*sarama.ConsumerMessage, len(inMessages))
	for i, msg := range inMessages {
		msg.Value = []byte(string(msg.Value) + "*")
		outMessages[i] = msg
	}
	return outMessages
}
