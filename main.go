package main

import (
	"logtransfer/commons"
	"logtransfer/es"
	"logtransfer/kafka"

	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
)

// log transfer
// 从kafka消费日志输入，写入ES

func main() {
	// 初始化配置文件
	var cfg = new(commons.Config)
	err := ini.MapTo(cfg, "./config/logtransfer.ini")
	if err != nil {
		logrus.Errorf("load config file failed, %v", err)
		return
	}
	logrus.Info("load config file success")

	// 连接kafka，获得指定Topic的所有分区的消费者列表
	partitionConsumerList, err := kafka.CreateConsumer([]string{cfg.KafkaConfig.Address}, cfg.KafkaConfig.Topic)
	if err != nil {
		logrus.Errorf("connect to kafka failed, %v", err)
		return
	}
	logrus.Info("connect to kafka success")
	// 生成一个chan用于保存数据
	msgChan := make(chan string, 500)
	// 消费者从kafka中消费数据发送到msgChan
	kafka.GetMsgFromKafka(partitionConsumerList, msgChan)

	// 连接es
	esClient, err := es.ESInit(cfg.ESConfig.Address)
	if err != nil {
		logrus.Errorf("connect to es failed, %v", err)
		return
	}
	logrus.Info("connect to es success")
	// 设置es的index
	esClient.SetIndex(cfg.ESConfig.Index)
	// 保存msgChan中的数据到es
	esClient.SaveData(msgChan)

	// mem := new(runtime.MemStats)
	// go func() {
	// 	for {
	// 		select {
	// 		case <-time.After(time.Second):
	// 			runtime.ReadMemStats(mem)
	// 			fmt.Println("当前Goroutine数量:", runtime.NumGoroutine())
	// 			fmt.Printf("申请并在使用的字节数:%v, 申请内存的次数:%v, 从系统中获取的字节数:%v\n", mem.Alloc, mem.Mallocs, mem.Sys)
	// 		}
	// 	}
	// }()

	select {}
}
