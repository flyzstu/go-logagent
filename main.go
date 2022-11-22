package main

import (
	"fmt"
	"logagent/conf"
	"logagent/es"
	"logagent/etcd"
	"logagent/taillog"
	"logagent/utils"
	"time"

	"github.com/flyzstu/mylog"
	"gopkg.in/ini.v1"
)

var (
	cfg    = new(conf.AppConf)
	logger = mylog.New()
)

func main() {
	defer logger.Close()

	//0. 初始化加载配置文件
	err := ini.MapTo(cfg, "config.ini")
	if err != nil {
		// fmt.Println("load ini failed, err:", err.Error())
		logger.Error("读取配置文件失败, 错误:%s", err.Error())
		return
	}
	logger.Info("成功读取配置文件")

	//1. 初始化kafka链接
	// err = kafka.Init([]string{cfg.KafkaConf.Address}, cfg.Size)
	// if err != nil {
	// 	logger.Error("初始化日志失败, 错误:%s", err.Error())
	// 	return
	// }
	// logger.Info("成功连接到kafka")

	// 初始化ES链接
	err = es.Init(cfg.ESConf.Address)
	if err != nil {
		logger.Error("初始化ES失败, 错误:%s", err.Error())
		return
	}
	logger.Info("初始化ES成功")
	//2. 初始化etcd
	err = etcd.Init([]string{cfg.EtcdConf.Address}, cfg.Timeout*time.Second)
	if err != nil {
		logger.Error("初始化ETCD, 错误:%s", err.Error())
		return
	}
	logger.Info("成功连接到etcd")

	// 2.1 从etcd中获取日志收集项的配置信息

	// 根据ip合成需要收集日志的key
	localIP, err := utils.GetOutboundIP()
	if err != nil {
		panic(err)
	}
	key := fmt.Sprintf(cfg.Key, localIP)
	logEntryConf, err := etcd.GetConf(key)
	if err != nil {
		logger.Error("从etcd拉取配置失败, 错误:%s", err.Error())
		return
	}
	logger.Info("从etcd拉取配置成功")
	taillog.Init(logEntryConf)

	go etcd.WatchConf(key, taillog.NewConfChan())
	select {}
}
