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
	consumerWg   *sync.WaitGroup
	msgWg        *sync.WaitGroup
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
		consumerWg:   &sync.WaitGroup{},
		msgWg:        &sync.WaitGroup{},
	}
}

// 消费数据: 只要成功将数据丢给消费者，就是放一个生产者的限制，而不需要等待消费任务完成再释放，做到生产和消费分离
func (mq *coolMQ) work() {
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
	Topic         string
	ProducerLimit int
	ConsumerLimit int
	Consumer      func(msg *Msg)
	CloseTrigger  func(topic string)
}

// ========================== mq 中心 ==========================
type MqBus struct {
	mapMQ   *sync.Map
	topicWg *sync.WaitGroup
}

func NewMqBus() *MqBus {
	return &MqBus{
		mapMQ:   &sync.Map{},
		topicWg: &sync.WaitGroup{},
	}
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
	if !has(bus, mqConf.Topic) {
		mq := newCoolMQ(mqConf.Topic, mqConf.ProducerLimit, mqConf.ConsumerLimit, mqConf.Consumer)
		mq.onClose = mqConf.CloseTrigger
		bus.mapMQ.Store(mqConf.Topic, mq)
		bus.topicWg.Add(1)
		go mq.work()
	}
}

// 消费完一个数据，通知
func Done(bus *MqBus, topic string) {
	if has(bus, topic) {
		mq := getMq(bus, topic)
		mq.msgWg.Done()
		mq.consumerWg.Done()
		// 释放 producer 以及 consumer 的限制
		<-mq.producerChan
		<-mq.consumerChan
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
		mq.msgWg.Add(1) // 有时生产数据的过程过于缓慢
		mq.producerChan <- true
		go func(mq *coolMQ) {
			msg := &Msg{
				Topic: topic,
				Data:  data,
			}
			mq.dataChan <- msg
		}(mq)
	}
}

// 停止指定topic
func Stop(bus *MqBus, topics ...string) {
	for _, topic := range topics {
		if has(bus, topic) {
			go func(topic string) {
				mq := getMq(bus, topic)
				mq.msgWg.Wait()      // 等待所有的消息 被拿出来
				mq.consumerWg.Wait() // 等待所有的 consumer 完成
				close(mq.dataChan)
				if mq.onClose != nil {
					mq.onClose(topic)
				}
				bus.mapMQ.Delete(topic)
				bus.topicWg.Done()
			}(topic)
		}
	}
}
