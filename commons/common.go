package commons

import "github.com/Shopify/sarama"

// kafka 配置文件
type KafkaConfig struct {
	Address string `ini:"address"`
	Topic   string `ini:"topic"`
}

// es配置文件
type ESConfig struct {
	Address     string `ini:"address"`
	Index       string `ini:"index"`
	MaxChanSize int    `ini:"max_chan_size"`
}

// 总配置文件
type Config struct {
	KafkaConfig `ini:"kafka"`
	ESConfig    `ini:"es"`
}

// kafka消费者封装结构体
type KafkaConsumer struct {
	// 分区的消费者
	ComsumerPartition sarama.PartitionConsumer

	// 用于指示消费者是否应该关闭
	CloseChan chan struct{}
}
