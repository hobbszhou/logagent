package main

import (
	"fmt"
	"logagent/common"
	"logagent/etcd"
	"logagent/kafka"
	"logagent/sysinfo"
	"logagent/tailfile"
	"logagent/utils"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
)

var (
	log *logrus.Logger
	wg  sync.WaitGroup
)

type config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
	EtcdConfig    `ini:"etcd"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	ChanSize int    `ini:"chan_size"`
}

type CollectConfig struct {
	Logfile string `ini:"logfile"`
}

type EtcdConfig struct {
	Address           string `ini:"address"`
	CollectLogKey     string `ini:"collect_log_key"`
	CollectSysInfoKey string `ini:"collect_sysinfo_key"`
}

func initLogger() {
	log = logrus.New()
	
	log.Out = os.Stdout
	log.Level = logrus.DebugLevel
	
	log.Info("init log success")
}

func run(logConfKey string, sysinfoConf *common.CollectSysInfoConfig) {
	
	wg.Add(2)
	go etcd.WatchConf(logConfKey)
	go sysinfo.Run(time.Duration(sysinfoConf.Interval)*time.Second, sysinfoConf.Topic)
	wg.Wait()
}

func main() {
	initLogger()
	var cfg config 
	
	err := ini.MapTo(&cfg, "./conf/config.ini")
	if err != nil {
		panic(fmt.Sprintf("init config failed, err:%v", err))
	}
	
	err = kafka.Init(strings.Split(cfg.KafkaConfig.Address, ","), cfg.KafkaConfig.ChanSize)
	if err != nil {
		panic(fmt.Sprintf("init kafka failed, err:%v", err))
	}

	ip, err := utils.GetOutboundIP()
	if err != nil {
		panic(fmt.Sprintf("get local ip failed, err:%v", err))
	}
	
	collectLogKey := fmt.Sprintf(cfg.EtcdConfig.CollectLogKey, ip)
	err = etcd.Init(strings.Split(cfg.EtcdConfig.Address, ","), collectLogKey)
	if err != nil {
		panic(fmt.Sprintf("init etcd failed, err:%v", err))
	}
	log.Debug("init etcd success!")

	collectLogConf, err := etcd.GetConf(collectLogKey)
	if err != nil {
		panic(fmt.Sprintf("get collect conf from etcd failed, err:%v", err))
	}
	log.Debugf("111111111111----%s", collectLogConf)

	collectSysinfoKey := fmt.Sprintf(cfg.EtcdConfig.CollectSysInfoKey, ip)
	collectSysinfoConf, err := etcd.GetSysinfoConf(collectSysinfoKey)
	fmt.Printf("collectSysinfoConf====%s\n", collectSysinfoConf)
	if err != nil {
		panic(fmt.Sprintf("get collect sys info conf from etcd failed, err:%v", err))
	}
	if collectSysinfoConf == nil {
		collectSysinfoConf = &common.CollectSysInfoConfig{
			Interval: 5,
			Topic:    "collect_system_info",
		}
	}
	log.Debugf("%#v", collectSysinfoConf)

	newConfChan := etcd.WatchChan()
	
	err = tailfile.Init(collectLogConf, newConfChan) 
	if err != nil {
		panic(fmt.Sprintf("init tail failed, err:%v", err))
	}
	log.Debug("init tail success!")

	run(collectLogKey, collectSysinfoConf)
	log.Debug("logagent exit")
}
