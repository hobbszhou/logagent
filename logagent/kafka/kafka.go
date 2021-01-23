package kafka

import (
	"code.oldboyedu.com/logagent/conf"
	"fmt"
	"github.com/Shopify/sarama"
)

// 专门往kafka写日志的模块

var (
	client  sarama.SyncProducer // 声明一个全局的连接kafka的生产者client
	msgChan chan *sarama.ProducerMessage
)

// Init 初始化client
func Init(addrs []string) (err error) {
	cfg := conf.GetConfInstance()
	config := sarama.NewConfig()
	// tailf包使⽤
	config.Producer.RequiredAcks = sarama.WaitForAll          // 发送完数据需要leader和follow都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 新选出⼀个 partition
	config.Producer.Return.Successes = true                   // 成功交付的消息将在success channel返回

	// 连接kafka
	client, err = sarama.NewSyncProducer(addrs, config)
	if err != nil {
		fmt.Println("producer closed, err:", err)
		return
	}
	msgChan = make(chan *sarama.ProducerMessage, cfg.KafkaConf.ChanSize)
	// 起一个 gorutine 从msgchan中读取数据
	go SendToKafka()
	return
}

func SendToKafka() {
	fmt.Println("-----------SendToKafka---------")
	for {
		select {
		case msg := <-msgChan:
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				fmt.Println("error msg failed, err:", err)
				return
			}
			fmt.Println("send success , pid=", pid, " offset=", offset)
		}

	}
}
func ToMsgChan(msg *sarama.ProducerMessage) {
	msgChan <- msg

}
