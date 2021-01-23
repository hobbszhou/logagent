package main

import (
	"code.oldboyedu.com/logagent/conf"
	"code.oldboyedu.com/logagent/etcd"
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
	/// 制造一个死循环
	select {}
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

	// 初始化 etcd 连接
	// 5 * time.Second
	err = etcd.Init(cfg.EtcdConf.Address, time.Duration(cfg.EtcdConf.Timeout)*time.Second)
	if err != nil {
		fmt.Printf("init etcd failed,err:%v\n", err)
		return
	}
	fmt.Println("init etcd success.")
	// 2.1 从etcd中获取日志收集项的配置信息
	logEntryConf, err := etcd.GetConf(cfg.EtcdConf.CollectKey)
	// 2.2 拍一个哨兵去监视日志收集项的变化（有变化及时通知我的logAgent实现热加载配置）
	if err != nil {
		fmt.Printf("etcd.GetConf failed,err:%v\n", err)
		return
	}
	fmt.Printf("get conf from etcd success, %v\n", logEntryConf)
	// 2. 根据etcd获取的配置进行初始化
	err = taillog.Init(logEntryConf)

	for index, value := range logEntryConf {
		fmt.Printf("index:%v value:%v\n", index, value)
	}
	////2. 打开日志文件准备收集日志
	//err = taillog.Init(cfg.TaillogConf.FileName)
	//if err != nil {
	//	fmt.Printf("Init taillog failed,err:%v\n", err)
	//	return
	//}
	//fmt.Println("init taillog success.")
	////3. 具体的业务
	run()

}
