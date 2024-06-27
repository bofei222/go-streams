package main

import (
	"context"
	"encoding/json"
	"fmt"
	ex "github.com/reugn/go-streams/extension"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
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
	groupID := "testConsumer-bf28"

	source, err := ext.NewKafkaSource(ctx, hosts, groupID, config, "modbus")
	if err != nil {
		log.Fatal(err)
	}

	parseJsonFlow := flow.NewMap(parseJson, 1)
	// 1s 的，本周期 采集的全部 风机的数据。  []map[string]interface{}
	slidingWindow := flow.NewSlidingWindowWithExtractor[map[string]interface{}](
		3000*time.Millisecond,
		3000*time.Millisecond,
		func(m map[string]interface{}) int64 {
			return m["eventTime"].(int64)
		})
	slidingWindowH := flow.NewSlidingWindowWithExtractor[map[string]interface{}](
		10000*time.Millisecond,
		10000*time.Millisecond,
		func(m map[string]interface{}) int64 {
			return m["eventTime"].(int64)
		})
	//	return string(msg.Value)
	//}
	//fileSink := ex.NewFileSink("out.txt")
	out := make(chan any, 1000)
	chanSink := ex.NewChanSink(out)
	//stdoutSink := ex.NewStdoutSink()
	xxFlow := flow.NewMap(xx, 1)
	yyFlow := flow.NewMap(yyMap, 1)

	source.
		Via(flow.NewPassThrough()).
		Via(parseJsonFlow).
		Via(slidingWindow).
		Via(xxFlow).
		Via(yyFlow).
		To(chanSink)

	chanSource := ex.NewChanSource(chanSink.Out)
	chanSource.
		Via(flow.NewPassThrough()).
		Via(slidingWindowH).
		To(ex.NewStdoutSink())

}

var parseJson = func(msg *sarama.ConsumerMessage) map[string]interface{} {
	var data map[string]interface{}
	err := json.Unmarshal(msg.Value, &data)
	if err != nil {
		log.Printf("Error parsing JSON: %v", err)
		return data
	}
	for k, v := range data {
		if strings.HasPrefix(k, "staCode") == false {
			if !strings.Contains(k, "innerTurbineName") {
				data[k], _ = strconv.ParseFloat(v.(string), 64)
			}

		}
	}
	data["eventTime"] = int64(data["collectTime"].(float64)) * 1000000000

	//fmt.Println(data)
	return data
}

var xx = func(msg []map[string]interface{}) []map[string]interface{} {
	// 遍历msg
	for _, m := range msg {
		// 遍历当前 map 的 key 和 value
		for key, value := range m {
			fmt.Printf(": %s, : %v ", key, value)
		}
		fmt.Println()
	}
	return msg
}

var yyMap = func(msg []map[string]interface{}) map[string]map[string]interface{} {
	result := make(map[string]map[string]interface{})
	// 遍历msg
	for _, m := range msg {
		// m["innerTurbineName"] 转为string
		//m["innerTurbineName"] = strconv.FormatFloat(m["innerTurbineName"].(float64), 'f', -1, 64)
		result[m["innerTurbineName"].(string)] = m
	}
	fmt.Println(result)
	return result
}
