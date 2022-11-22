package etcd

import (
	"context"
	"fmt"
	"time"

	"github.com/goccy/go-json"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 需要收集的日志信息
type LogEntry struct {
	Path  string `json:"path"`  // 路径
	Topic string `json:"topic"` // 发往kafka的topic
}

var (
	client = new(clientv3.Client)
)

func Init(address []string, timeout time.Duration) (err error) {

	client, err = clientv3.New(
		clientv3.Config{
			Endpoints:   address,
			DialTimeout: timeout,
		})
	if err != nil {
		return
	}
	return
}

// 从etcd中获取配置项
func GetConf(key string) (logEntryConf []*LogEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := client.Get(ctx, key)
	defer cancel()
	if err != nil {
		return
	}

	for _, ev := range resp.Kvs {
		// fmt.Printf("%s:%s\n", ev.Key, ev.Value)
		err = json.Unmarshal(ev.Value, &logEntryConf)
		if err != nil {
			return
		}
	}
	return
}

// etcd watch
func WatchConf(key string, newConfCh chan<- []*LogEntry) {
	rch := client.Watch(context.Background(), key)
	for wresp := range rch {
		for _, ev := range wresp.Events {
			// fmt.Printf("Type: %s, Key:%s, value: %s\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			var newConf []*LogEntry

			if ev.Type != clientv3.EventTypeDelete {
				err := json.Unmarshal(ev.Kv.Value, &newConf)
				if err != nil {
					fmt.Println("unmarshal failed, err:", err.Error())
					continue
				}
				// fmt.Printf("get new conf: %#v\n", newConf)
			}
			newConfCh <- newConf
		}
	}
}
