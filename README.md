# logagent 一个日志收集项目
### 一、环境搭建
##### 1. 配置jdk
##### 2. 安装zookeeper
##### 3. 安装kafka

## 二、启动环境

##### 1. 启动zookeper
``` bash
   zookeeper-server-start.bat ..\..\config\zookeeper.properties
```
##### 2. 启动kafka
``` bash
   kafka-server-start.bat ..\..\config\server.properties
```

##### 3. 创建主题
``` bash
   .\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
```
##### 4. 查看主题输入
``` bash
   .\bin\windows\kafka-topics.bat --list --zookeeper localhost:2181
```
##### 5. 创建生产者
``` bash
   .\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic test
```
##### 6. 创建消费者
``` bash
   .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --from-beginning
   ```
