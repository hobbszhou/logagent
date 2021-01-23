package taillog

import (
	"code.oldboyedu.com/logagent/common"
	"code.oldboyedu.com/logagent/kafka"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"strings"
	"time"
)

// 专门从日志文件收集日志的模块

var (
	TailObj *tail.Tail
	LogChan chan string
)

type TailTask struct {
	path  string
	topic string
	tobj  *tail.Tail
}

func newTailTask(path, topic string) *TailTask {
	tailObj := &TailTask{
		path:  path,
		topic: topic,
	}
	return tailObj
}
func Init(logEntryConf []common.LogEntry) (err error) {

	for _, conf := range logEntryConf {
		fmt.Printf("--------2222---conf.Path=%s----conf.Topic=%s\n", conf.Path, conf.Topic)
		tt := newTailTask(conf.Path, conf.Topic)
		err = tt.Init()
		if err != nil {
			fmt.Printf("create tailobj for path=%s, failed, err=%s\n", conf.Path, err)
		}
		fmt.Printf("create a tail task for path=%s success\n", conf.Path)
		go tt.run()
	}

	//TailObj, err = tail.TailFile(fileName, config)
	//if err != nil {
	//	fmt.Println("tail file failed, err:", err)
	//	return
	//}
	return
}
func (t *TailTask) Init() (err error) {
	cfg := tail.Config{
		ReOpen:    true,                                 // 重新打开
		Follow:    true,                                 // 是否跟随
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // 从文件的哪个地方开始读
		MustExist: false,                                // 文件不存在不报错
		Poll:      true,
	}
	t.tobj, err = tail.TailFile(t.path, cfg)

	return err
}
func ReadChan() <-chan *tail.Line {
	return TailObj.Lines
}
func (t *TailTask) run() {
	fmt.Printf("collct for path:%s is runing....\n", t.path)

	// 1. 读取日志
	fmt.Println("------------ run ------------")
	fmt.Println("filenmae=", t.tobj.Filename, t.topic)
	for {
		line, ok := <-t.tobj.Lines
		if !ok {
			fmt.Println("tail file close reopen ,filename is=", t.tobj.Filename)
			time.Sleep(time.Second)
			continue
		}
		if len(strings.Trim(line.Text, "\r")) == 0 {
			fmt.Println("出现换行直接跳过..")
			continue
		}

		fmt.Println("line=", line.Text)
		// 改为异步，利用通道
		msg := &sarama.ProducerMessage{}
		msg.Topic = t.topic
		msg.Value = sarama.StringEncoder(line.Text)
		kafka.ToMsgChan(msg)
	}
}
