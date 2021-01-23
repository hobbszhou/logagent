package etcd

import (
	"code.oldboyedu.com/logagent/common"
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"time"
)

var (
	cli *clientv3.Client
)

// 初始化ETCD的函数
func Init(addr string, timeout time.Duration) (err error) {
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: timeout,
	})
	if err != nil {
		// handle error!
		fmt.Printf("connect to etcd failed, err:%v\n", err)
		return
	}
	return
}

// 从ETCD中根据key获取配置项
func GetConf(key string) (logEntryConf []*common.LogEntry, err error) {

	fmt.Println("key==========", key)
	// get

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, key)
	cancel()
	if err != nil {
		fmt.Printf("get from etcd failed, err:%v\n", err)
		return
	}
	if len(resp.Kvs) == 0 {
		fmt.Println("failed: get len:0 conf from etcd by key:%s", key)
	}
	ret := resp.Kvs[0]

	// ret.Value // json格式化字符串
	fmt.Println(ret.Value)
	err = json.Unmarshal(ret.Value, &logEntryConf)
	fmt.Println("vvvvvvvv=", logEntryConf)
	if err != nil {
		fmt.Println("failed, json unmarshal failed, err:%v", err)
		return
	}

	//for _, ev := range resp.Kvs {
	//	//fmt.Printf("%s:%s\n", ev.Key, ev.Value)
	//	err = json.Unmarshal(ev.Value, &logEntryConf)
	//	if err != nil {
	//		fmt.Printf("unmarshal etcd value failed,err:%v\n", err)
	//		return
	//	}
	//}
	return
}

// C:/tmp/nginx.log   web_log
// D:/xxx/redis.log   redis_log
