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
	onClose      func(topic string)
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
	log.Trace(msg)
	for d := range mq.dataChan {
		mq.consumerChan <- true
		mq.consumerWg.Add(1)
		go mq.consumer(d)
	}
	msg = "关闭mq数据通道: " + mq.topic
	log.Trace(msg)
}

// ========================== mq 配置 ==========================
type MqConfig struct {
	topic         string
	producerLimit int
	consumerLimit int
	consumer      func(msg *Msg)
	closeTrigger  func(topic string)
}

// ========================== mq 中心 ==========================
type MqBus struct {
	mapMQ        *sync.Map
	producerChan chan bool
	topicWg      *sync.WaitGroup
}

func NewMqBus() *MqBus {
	return &MqBus{
		mapMQ:        &sync.Map{},
		producerChan: make(chan bool),
		topicWg:      &sync.WaitGroup{},
	}
}

// 控制整体生产数据的速度，调大理论上可以加速
func SetProducerLimit(bus *MqBus, producerLimit int) {
	bus.producerChan = make(chan bool, producerLimit)
}

func has(bus *MqBus, topic string) bool {
	if _, ok := bus.mapMQ.Load(topic); ok {
		return true
	}
	return false
}

func getMq(bus *MqBus, topic string) *coolMQ {
	v, ok := bus.mapMQ.Load(topic)
	if ok {
		return v.(*coolMQ)
	}
	return nil
}

func AddTopic(bus *MqBus, mqConf *MqConfig) {
	if !has(bus, mqConf.topic) {
		mq := newCoolMQ(mqConf.topic, mqConf.producerLimit, mqConf.consumerLimit, mqConf.consumer)
		mq.onClose = mqConf.closeTrigger
		bus.mapMQ.Store(mqConf.topic, mq)
		bus.topicWg.Add(1)
		go mq.consume()
	}
}

// 消费完一个数据，通知
func Done(bus *MqBus, topic string) {
	if has(bus, topic) {
		mq := getMq(bus, topic)
		mq.msgWg.Done()
		mq.consumerWg.Done()
		<-mq.producerChan
		// 释放 producer 以及 consumer 的限制
		<-mq.consumerChan
		<-bus.producerChan
	}
}

func Wait(bus *MqBus) {
	bus.topicWg.Wait()
	log.Trace("所有 TopicMQ 已关闭")
}

// 生产数据
func Produce(bus *MqBus, topic string, data any) {
	if has(bus, topic) {
		mq := getMq(bus, topic)
		if mq.consumer == nil {
			return
		}
		bus.producerChan <- true // 控制整体的并发数量：防止oom
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

// 关闭指定topic
func Close(bus *MqBus, topics ...string) {
	for _, topic := range topics {
		if has(bus, topic) {
			mq := getMq(bus, topic)
			mq.msgWg.Wait()      // 等待 msg 都被消费
			mq.consumerWg.Wait() // 等待所有的 consumer 完成
			close(mq.dataChan)
			if mq.onClose != nil {
				mq.onClose(topic)
			}
			bus.mapMQ.Delete(topic)
			bus.topicWg.Done()
		}
	}
}
