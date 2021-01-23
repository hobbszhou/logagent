package conf

import (
	"fmt"
	"gopkg.in/ini.v1"
	"log"

	"sync"
)

var rdsLock sync.Mutex
var cacheInstance *AppConf

type AppConf struct {
	KafkaConf   `ini:"kafka"`
	TaillogConf `ini:"taillog"`
}
type KafkaConf struct {
	Address  string `ini:"address"`
	Topic    string `ini:"topic"`
	ChanSize int64  `ini:"chanSize"`
}

type TaillogConf struct {
	FileName string `ini:"fileName"`
}

func GetConfInstance() *AppConf {
	if cacheInstance != nil {
		return cacheInstance
	}
	rdsLock.Lock()
	defer rdsLock.Unlock()

	if cacheInstance != nil {
		return cacheInstance
	}
	return newCache()
}
func newCache() *AppConf {
	cfg, err := ini.Load("./conf/config.ini")
	if err != nil {
		log.Fatal("laod conf failed:", err)
	}
	c := new(AppConf)
	cfg.MapTo(c)
	cacheInstance = c
	fmt.Println("cacheInstance=", cacheInstance)
	return cacheInstance
}
