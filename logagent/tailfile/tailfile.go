package tailfile

import (
	"context"
	"encoding/json"
	"fmt"
	"logagent/kafka"
	"logagent/utils"
	"os"

	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
)

var (
	log     *logrus.Logger
	localIP string
)

type LogData struct {
	IP   string `json:"ip"`
	Data string `json:"data"`
}

func init() {
	log = logrus.New()

	log.Out = os.Stdout
	log.Level = logrus.DebugLevel

	log.Info("etcd:init log success")
	var err error
	localIP, err = utils.GetOutboundIP()
	if err != nil {
		log.Errorf("get local ip failed,err:%v", err)
	}
}

type tailObj struct {
	path     string
	module   string
	topic    string
	instance *tail.Tail
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewTailObj(path, module, topic string) (tObj *tailObj, err error) {
	tObj = &tailObj{
		path:   path,
		module: module,
		topic:  topic,
	}
	ctx, cancel := context.WithCancel(context.Background())
	tObj.ctx = ctx
	tObj.cancel = cancel
	err = tObj.Init()
	return
}

func (t *tailObj) Init() (err error) {
	t.instance, err = tail.TailFile(t.path, tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	})
	if err != nil {
		fmt.Println("init tail failed, err:", err)
		return
	}
	return
}

func (t *tailObj) run() {
	for {
		select {
		case <-t.ctx.Done():
			log.Warnf("the task for path:%s is stop...", t.path)
			return
		case line, ok := <-t.instance.Lines:
			if !ok {
				log.Errorf("read line failed")
				continue
			}
			data := &LogData{
				IP:   localIP,
				Data: line.Text,
			}
			jsonData, err := json.Marshal(data)
			if err != nil {
				log.Warningf("unmarshal tailfile.LodData failed, err:%v", err)
			}
			msg := &kafka.Message{
				Data:  string(jsonData),
				Topic: t.topic,
			}
			err = kafka.SendLog(msg)
			if err != nil {
				log.Errorf("send to kafka failed, err:%v\n", err)
			}
		}
		log.Debug("send msg to kafka success")
	}
}

func (t *tailObj) ReadLine() (line *tail.Line, err error) {
	var ok bool
	line, ok = <-t.instance.Lines
	if !ok {
		err = fmt.Errorf("read line failed, err:%v", err)
		return
	}
	return
}
