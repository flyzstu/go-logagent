package taillog

import (
	"context"
	"fmt"
	"logagent/es"
	"time"

	"github.com/hpcloud/tail"
)

// 一个日志收集的任务
type TailTask struct {
	path       string
	topic      string
	instance   *tail.Tail
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func (t *TailTask) Init() (err error) {
	// t.instance = new(tail.Tail)
	t.instance, err = tail.TailFile(t.path, tail.Config{
		ReOpen:    true,                                 // 重新打开
		Follow:    true,                                 // 跟随文件
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // END of FILE
		MustExist: false,
		Poll:      true,
	})
	if err != nil {
		fmt.Println("tail file failed, error: ", err.Error())
		return
	}
	//启用后台干活程序, 开始干活
	go t.run()
	return
}

// 构造函数
func NewTailTask(path, topic string) (tailtask *TailTask) {
	ctx, cancel := context.WithCancel(context.Background())
	tailtask = &TailTask{
		path:       path,
		topic:      topic,
		ctx:        ctx,
		cancelFunc: cancel,
	}
	tailtask.Init() // 根据路径打开对应的日志
	return
}

// 干活的函数
func (t *TailTask) run() {
	for {
		select {
		case <-t.ctx.Done():
			fmt.Println("任务结束: ", t.topic+"_"+t.path)
			return
		case line := <-t.instance.Lines:
			// 把数据发到通道中，通过通道发送
			// fmt.Printf("get log from %s, log: %s\n", t.path, line.Text)
			// kafka.SendToChan(t.topic, line.Text)
			es.SendMessageToChan(t.topic, line.Text)
			// kafka.SendToKafka(t.topic, line.Text) // 函数调用函数，比较慢
		default:
			time.Sleep(time.Second)
		}
	}
}

func (t *TailTask) ReadChan() <-chan *tail.Line {
	return t.instance.Lines
}
