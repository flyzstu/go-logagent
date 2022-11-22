package es

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/esapi"
)

type LogData struct {
	data  string
	topic string
}

var (
	es             *elasticsearch.Client
	logMessageChan = make(chan *LogData, 100000)
)

func Init(address string) error {
	cfg := elasticsearch.Config{
		Addresses: []string{fmt.Sprintf("http://%s", address)},
	}
	var err error
	es, err = elasticsearch.NewClient(cfg)
	if err != nil {
		return err

	}

	res, err := es.Info()
	if err != nil {
		return err
	}
	defer res.Body.Close()
	// fmt.Println(res.String())
	go worker()
	return nil
}

// 后台干活的函数
func worker() {
	for message := range logMessageChan {
		fmt.Printf("data: %v\n", message)
		req := esapi.IndexRequest{
			Index:        "log",
			DocumentType: message.topic,
			Body:         bytes.NewReader([]byte(message.data)),
		}

		res, err := req.Do(context.Background(), es)
		if err != nil {
			log.Fatalf("Error getting response: %s", err)
		}
		if res.IsError() {
			log.Fatalf("res.Status(): %s", res.Status())
		}
	}
}

func SendMessageToChan(topic, data string) {
	logMessageChan <- &LogData{
		data:  data,
		topic: topic,
	}
}
