package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

type LogData struct {
	topic string
	data  string
}

var (
	client      sarama.SyncProducer
	logDataChan chan *LogData
)

func Init(addrs []string, channelSize int) (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal        // 发送完数据需要
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 新选出一个leader
	config.Producer.Return.Successes = true                   // 成功交付的消息将在success_channel返回

	// 链接kafka
	client, err = sarama.NewSyncProducer(addrs, config)
	if err != nil {
		fmt.Println("make message failed, err:", err.Error())
		return err
	}
	// 初始化全局channel
	logDataChan = make(chan *LogData, channelSize)
	go sendToKafka()
	return nil
}

func SendToChan(topic, data string) {
	logDataChan <- &LogData{
		topic: topic,
		data:  data,
	}
}

// 真正往kafka里面发送日志的函数
func sendToKafka() {
	for logData := range logDataChan {
		kafkaMsg := &sarama.ProducerMessage{}
		kafkaMsg.Topic = logData.topic
		kafkaMsg.Value = sarama.StringEncoder(logData.data)

		// 发送到kafla
		pid, offset, err := client.SendMessage(kafkaMsg)
		if err != nil {
			fmt.Println("send message failed, err:", err.Error())
			return
		}
		fmt.Printf("消息发送成功, pid:%v offset:%v\n", pid, offset)
	}

}
