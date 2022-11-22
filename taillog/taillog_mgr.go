package taillog

import (
	"fmt"
	"logagent/etcd"
	"time"
)

var (
	tskMgr *TailLogMgr
)

// tailTask管理者
type TailLogMgr struct {
	logEntry    []*etcd.LogEntry
	tskMap      map[string]*TailTask
	newConfChan chan []*etcd.LogEntry
}

func Init(logEntryConf []*etcd.LogEntry) {
	tskMgr = &TailLogMgr{
		logEntry:    logEntryConf,
		tskMap:      make(map[string]*TailTask, 12),
		newConfChan: make(chan []*etcd.LogEntry), // 无缓冲区通道
	}
	for _, logEntry := range logEntryConf {
		// fmt.Printf("topic:%s, path:%s\n", logEntry.Topic, logEntry.Path)
		// 记录一下起了多少个task

		tailObj := NewTailTask(logEntry.Path, logEntry.Topic)
		// mkey := logEntry.Topic + "/" + logEntry.Path
		mkey := fmt.Sprintf("%s_%s", logEntry.Topic, logEntry.Path)
		tskMgr.tskMap[mkey] = tailObj

	}
	go tskMgr.update()
}

// 监听自己的newConfChan, 有了新的配置就处理

func (t *TailLogMgr) update() {
	for {
		select {
		case newConf := <-t.newConfChan:
			for _, conf := range newConf {
				mkey := fmt.Sprintf("%s_%s", conf.Topic, conf.Path)
				if _, ok := tskMgr.tskMap[mkey]; ok {
					// 任务包含当前path收集的日志
					continue
				} else {
					// 新增的或者修改的
					tailObj := NewTailTask(conf.Path, conf.Topic)
					t.tskMap[mkey] = tailObj
				}
			}

			// 找出原来t.logEntry中有的，但是newConf中没有的，关闭日志收集
			for _, c1 := range t.logEntry { // 从原来的配置中依次拿出配置
				isDelete := true
				for _, c2 := range newConf { //去新的配置中逐次比较
					if c2.Path == c1.Path && c2.Topic == c1.Topic {
						isDelete = false
						continue
					}
				}
				if isDelete {
					// 此时需要把这个gouroutine停掉
					mkey := fmt.Sprintf("%s_%s", c1.Topic, c1.Path)
					t.tskMap[mkey].cancelFunc()
				}
			}
			// 1. 配置新增
			// 2. 配置删除
			// 3. 配置变更
			// fmt.Printf("新的配置来了: %#v\n", newConf)
		default:
			time.Sleep(time.Second)
		}
	}
}

// 向外暴露 t.newConfChan
func NewConfChan() chan<- []*etcd.LogEntry {
	return tskMgr.newConfChan
}
