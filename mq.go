package coolmq

import (
	log "github.com/link-yundi/ylog"
	"sync"
)

/**
------------------------------------------------
Created on 2022-10-27 14:12
@Author: ZhangYundi
@Email: yundi.xxii@outlook.com
------------------------------------------------
**/

/**
多个主题，每个主题都有多个生产者，多个消费者，确保消费者消费完数据并且做完相应的工作才退出
*/

type Msg struct {
	Topic string
	Data  any
}

type coolMQ struct {
	topic        string
	dataChan     chan *Msg // 消息通道
	producerChan chan bool // 带缓存的channel,控制生产者的并发数量上限
	consumerChan chan bool // 带缓存的channel,控制消费者的并发数量上限
	consumer     func(msg *Msg)
	msgWg        *sync.WaitGroup // 用于保证msg不丢失：避免数据没来得及放入通道就被关闭
	consumerWg   *sync.WaitGroup // 用于保证所有的consumer完成
}

func newCoolMQ(topic string, producerLimit, consumerLimit int, consumer func(msg *Msg)) *coolMQ {
	chanLen := producerLimit
	if consumerLimit > chanLen {
		chanLen = consumerLimit
	}
	dataChan := make(chan *Msg, chanLen)
	producerChan := make(chan bool, producerLimit)
	consumerChan := make(chan bool, consumerLimit)
	return &coolMQ{
		topic:        topic,
		dataChan:     dataChan,
		producerChan: producerChan,
		consumerChan: consumerChan,
		consumer:     consumer,
		msgWg:        &sync.WaitGroup{},
		consumerWg:   &sync.WaitGroup{},
	}
}

// 消费数据: 只要成功将数据丢给消费者，就是放一个生产者的限制，而不需要等待消费任务完成再释放，做到生产和消费分离
func (mq *coolMQ) consume() {
	msg := "启动mq: " + mq.topic
	log.Info(msg)
	for d := range mq.dataChan {
		mq.consumerChan <- true
		mq.consumerWg.Add(1)
		go mq.consumer(d)
		mq.msgWg.Done()
		<-mq.producerChan
	}
	msg = "关闭mq数据通道: " + mq.topic
	log.Info(msg)
}

// ========================== mq 中心 ==========================
var (
	mapMQ        map[string]*coolMQ = map[string]*coolMQ{} // key: topic
	producerChan chan bool          = make(chan bool)      // 用于控制全部生产者的并发数量
	topicWg      *sync.WaitGroup    = &sync.WaitGroup{}    // 用于保证所有的topic都完成
)

// 控制整体生产数据的速度，调大理论上可以加速
func SetProducerLimit(producerLimit int) {
	producerChan = make(chan bool, producerLimit)
}

func has(topic string) bool {
	if _, ok := mapMQ[topic]; ok {
		return true
	}
	return false
}

func getMq(topic string) *coolMQ {
	if has(topic) {
		return mapMQ[topic]
	}
	return nil
}

func AddTopic(topic string, producerLimit, consumerLimit int, consumer func(msg *Msg)) {
	if !has(topic) {
		mapMQ[topic] = newCoolMQ(topic, producerLimit, consumerLimit, consumer)
	}
}

// 消费完一个数据，通知
func Done(topic string) {
	if has(topic) {
		mq := getMq(topic)
		mq.consumerWg.Done()
		// 释放 producer 以及 consumer 的限制
		<-mq.consumerChan
		<-producerChan
	}
}

func wait() {
	topicWg.Wait()
	log.Info("所有 TopicMQ 已关闭")
}

// 生产数据
func Produce(topic string, data any) {
	if has(topic) {
		mq := getMq(topic)
		producerChan <- true // 控制整体的并发数量：防止oom
		mq.msgWg.Add(1)
		go func(mq *coolMQ) {
			mq.producerChan <- true
			msg := &Msg{
				Topic: topic,
				Data:  data,
			}
			mq.dataChan <- msg
		}(mq)
	}
}

func Work() {
	for _, mq := range mapMQ {
		topicWg.Add(1)
		go mq.consume()
	}
}

// 关闭指定topic
func closeTopic(topic string) {
	if has(topic) {
		mq := getMq(topic)
		mq.msgWg.Wait()      // 等待 msg 都被消费
		mq.consumerWg.Wait() // 等待所有的 consumer 完成
		close(mq.dataChan)
		topicWg.Done()
		delete(mapMQ, topic)
	}
}

// 关闭所有topic
func Close() {
	for topic, _ := range mapMQ {
		closeTopic(topic)
	}
	wait()
}
