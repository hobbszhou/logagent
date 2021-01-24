package etcd

import (
	"context"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"logagent/common"
	"os"
	"time"

	"go.etcd.io/etcd/clientv3"
)

var (
	log      *logrus.Logger
	client   *clientv3.Client
	confChan chan []*common.CollectEntry
)

func init() {
	log = logrus.New()
	
	log.Out = os.Stdout
	log.Level = logrus.DebugLevel
	
	log.Info("etcd:init log success")
}

func Init(address []string, key string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		
		log.Errorf("connect to etcd failed, err:%v\n", err)
		return
	}
	confChan = make(chan []*common.CollectEntry)
	return
}

func GetConf(key string) (conf []*common.CollectEntry, err error) {
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := client.Get(ctx, key)
	defer cancel()
	if err != nil {
		log.Errorf("get from etcd failed, err:%v\n", err)
		return
	}
	if len(resp.Kvs) == 0 {
		log.Warnf("can't get any value by key:%s from etcd", key)
		return
	}
	keyValues := resp.Kvs[0]
	err = json.Unmarshal(keyValues.Value, &conf)
	if err != nil {
		log.Errorf("unmarshal value from etcd failed, err:%v", err)
		return
	}
	log.Debugf("load conf from etcd success, conf:%#v", conf)
	return
}

func GetSysinfoConf(key string) (conf *common.CollectSysInfoConfig, err error) {
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := client.Get(ctx, key)
	defer cancel()
	if err != nil {
		log.Errorf("get system info config from etcd failed, err:%v\n", err)
		return
	}
	if len(resp.Kvs) == 0 {
		log.Warnf("can't get any value by key:%s from etcd", key)
		return
	}
	keyValues := resp.Kvs[0]
	err = json.Unmarshal(keyValues.Value, &conf)
	if err != nil {
		log.Errorf("unmarshal value from etcd failed, err:%v", err)
		return
	}
	log.Debugf("load conf from etcd success, conf:%#v", conf)
	return
}

func WatchConf(key string) {
	for {
		rch := client.Watch(context.Background(), key) 
		log.Debugf("watch return, rch:%#v", rch)
		for wresp := range rch {
			if err := wresp.Err(); err != nil {
				log.Warnf("watch key:%s err:%v", key, err)
				continue
			}
			for _, ev := range wresp.Events {
				log.Debugf("Type: %s Key:%s Value:%s", ev.Type, ev.Kv.Key, ev.Kv.Value)
				
				var newConf []*common.CollectEntry
				
				if ev.Type == clientv3.EventTypeDelete {
					confChan <- newConf
					continue
				}
				err := json.Unmarshal(ev.Kv.Value, &newConf)
				if err != nil {
					log.Warnf("unmarshal the conf from etcd failed, err:%v", err)
					continue
				}
				confChan <- newConf
				log.Debug("send newConf to confChan success")
			}
		}
	}
}

func WatchChan() <-chan []*common.CollectEntry {
	return confChan
}
