package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
)

func main() {
	// 创建新的消费者
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		fmt.Println("fail to start cinsumer, err", err)
		return
	}
	// 拿到指定topic 下面的所有的分区列表
	partitionList, err := consumer.Partitions("test")
	if err != nil {
		fmt.Println("fail to get list of partition", err)
		return
	}
	fmt.Println("1111111111111111-----", partitionList)
	fmt.Printf("2222222222222\n")
	var wg sync.WaitGroup
	for partition := range partitionList { // 遍历所有的分区
		// 针对每个纷纷去船舰一个对应的分区消费之
		pc, err := consumer.ConsumePartition("test", int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d, err:%v\n", partition, sarama.OffsetNewest)
			return
		}
		defer pc.AsyncClose()
		// 异步从每个分区消费信息
		fmt.Printf("111111111111\n")
		wg.Add(1)
		go func(sarama.PartitionConsumer) {
			fmt.Printf("444444444444444")
			for msg := range pc.Messages() {
				fmt.Printf("5555555555\n")
				fmt.Printf("partition:%d offset:%d key:%d value:%v", msg.Partition, msg.Offset, msg.Key, msg.Value)
			}

		}(pc)
		fmt.Printf("333333333333\n")
	}
	wg.Wait()
}
