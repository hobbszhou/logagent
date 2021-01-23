package main

import (
	"code.oldboyedu.com/logagent/conf"
	"github.com/Shopify/sarama"
	"strings"
	"time"

	"code.oldboyedu.com/logagent/kafka"
	"code.oldboyedu.com/logagent/taillog"
	"fmt"
)

// logAgent入口程序

var (
	cfg = new(conf.AppConf)
)

func run() (err error) {
	// 1. 读取日志
	fmt.Println("------------ run ------------")
	fmt.Println("filenmae=", cfg.TaillogConf.FileName, cfg.KafkaConf.Topic)
	for {
		line, ok := <-taillog.TailObj.Lines
		if !ok {
			fmt.Println("tail file close reopen ,filename is=", taillog.TailObj.Filename)
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
		msg.Topic = cfg.KafkaConf.Topic
		msg.Value = sarama.StringEncoder(line.Text)
		kafka.ToMsgChan(msg)

	}
}

func main() {

	cfg = conf.GetConfInstance()
	fmt.Println(cfg)
	// 1. 初始化kafka连接
	err := kafka.Init([]string{cfg.KafkaConf.Address})
	if err != nil {
		fmt.Printf("init Kafka failed,err:%v\n", err)
		return
	}
	fmt.Println("init kafka success.")

	//2. 打开日志文件准备收集日志
	err = taillog.Init(cfg.TaillogConf.FileName)
	if err != nil {
		fmt.Printf("Init taillog failed,err:%v\n", err)
		return
	}
	fmt.Println("init taillog success.")
	//3. 具体的业务
	run()

}
