package kafka

import (
	"logtransfer/commons"

	"github.com/Shopify/sarama"
)

// 初始化kafka连接
func KafkaInit() (err error) {
	return
}

// 初始化消费者
//// 输入
// address：kafka IP
// topic：kafka topic
func CreateConsumer(address []string, topic string) (consumerList []commons.KafkaConsumer, err error) {
	// 创建新的消费者
	consumer, err := sarama.NewConsumer(address, nil)
	if err != nil {
		return nil, err
	}
	// defer consumer.Close()  // 只有该consumer的所有分区consumer全关闭后才能关闭

	// 获取指定topic下所有的分区列表
	partition, err := consumer.Partitions(topic)
	if err != nil {
		return nil, err
	}

	// 遍历所有分区，为每一个分区创建一个消费者
	consumerList = make([]commons.KafkaConsumer, len(partition), len(partition))
	for index, part := range partition {
		pc, err := consumer.ConsumePartition(topic, int32(part), sarama.OffsetNewest)
		if err != nil {
			return nil, err
		}
		consumerList[index] = commons.KafkaConsumer{
			ComsumerPartition: pc,
			CloseChan:         make(chan struct{}, 1),
		}
	}
	return consumerList, nil
}

// 从kafka中取出数据(开始消费)
func GetMsgFromKafka(consumerList []commons.KafkaConsumer, msgChan chan<- string) {
	for _, comsumer := range consumerList {
		go func(c commons.KafkaConsumer) {
			for {
				select {
				// 假如从CloseChan中取到了数据，则退出
				case <-c.CloseChan:
					c.ComsumerPartition.AsyncClose()
					close(msgChan)
					return
				// 持续性的从kafka中获取数据发送到msgChan
				case msg := <-c.ComsumerPartition.Messages():
					msgChan <- string(msg.Value)
				}
			}
		}(comsumer)
	}
}
