package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"os"
)

var (
	client  sarama.SyncProducer 
	msgChan chan *Message
	log     *logrus.Logger
)

type Message struct {
	Data  string
	Topic string
}

func init() {
	log = logrus.New()
	
	log.Out = os.Stdout
	log.Level = logrus.DebugLevel
	
	log.Info("kafka:init log success")
}

func Init(addrs []string, chanSize int) (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	client, err = sarama.NewSyncProducer(addrs, config)
	if err != nil {
		log.Errorf("producer closed, err:", err)
		return
	}
	msgChan = make(chan *Message, chanSize)
	go sendKafka()
	return
}

func SendLog(msg *Message) (err error) {
	select {
	case msgChan <- msg:
	default:
		err = fmt.Errorf("msgChan id full")
	}
	return
}

func sendKafka() {
	for msg := range msgChan {
		kafkaMsg := &sarama.ProducerMessage{}
		kafkaMsg.Topic = msg.Topic
		kafkaMsg.Value = sarama.StringEncoder(msg.Data)
		pid, offset, err := client.SendMessage(kafkaMsg)
		if err != nil {
			log.Warnf("send msg failed, err:%v\n", err)
			continue
		}
		log.Infof("send msg success, pid:%v offset:%v\n", pid, offset)
	}
}
