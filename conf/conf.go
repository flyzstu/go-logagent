package conf

import "time"

type KafkaConf struct {
	Address string `ini:"address"`
	Size    int    `ini:"channel_max_size"`
}

type ESConf struct {
	Address string `ini:"address"`
	Size    int    `ini:"channel_max_size"`
}

type TaillogConf struct {
	Path string `ini:"path"`
}
type EtcdConf struct {
	Address string        `ini:"address"`
	Timeout time.Duration `ini:"timeout"`
	Key     string        `ini:"collect_log_ley"`
}

type AppConf struct {
	KafkaConf   `ini:"kafka"`
	TaillogConf `ini:"taillog"`
	EtcdConf    `ini:"etcd"`
	ESConf      `ini:"es"`
}
