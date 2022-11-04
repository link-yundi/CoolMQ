package coolmq

import (
	"github.com/link-yundi/coolparallel"
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
	consumer     func(msg *Msg)
	onClose      func(topic string)
	producerPool *coolparallel.ParallelPool // 生产池
	consumerPool *coolparallel.ParallelPool // 消费池
}

func newCoolMQ(topic string, producerLimit, consumerLimit int, consumer func(msg *Msg)) *coolMQ {
	chanLen := producerLimit
	if consumerLimit > chanLen {
		chanLen = consumerLimit
	}
	dataChan := make(chan *Msg, chanLen)
	return &coolMQ{
		topic:        topic,
		dataChan:     dataChan,
		consumer:     consumer,
		producerPool: coolparallel.NewParallelPool(producerLimit),
		consumerPool: coolparallel.NewParallelPool(consumerLimit),
	}
}

// 监听
func (mq *coolMQ) listen() {
	msg := "启动mq: " + mq.topic
	log.Trace(msg)
	for d := range mq.dataChan {
		if mq.consumer != nil {
			mq.consumerPool.AddTask(mq.consume, d)
		}
	}
	msg = "关闭mq数据通道: " + mq.topic
	log.Trace(msg)
}

// 消费数据
func (mq *coolMQ) consume(arg any) {
	d, ok := arg.(*Msg)
	if !ok {
		return
	}
	mq.consumer(d)
}

// 生产数据
func (mq *coolMQ) produce(arg any) {
	msg, ok := arg.(*Msg)
	if !ok {
		return
	}
	mq.dataChan <- msg
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
		go mq.listen()
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
		msg := &Msg{
			Topic: topic,
			Data:  data,
		}
		mq.producerPool.AddTask(mq.produce, msg)
	}
}

// 停止指定topic
func Stop(bus *MqBus, topics ...string) {
	for _, topic := range topics {
		if has(bus, topic) {
			go func(topic string) {
				mq := getMq(bus, topic)
				mq.producerPool.Wait()
				mq.consumerPool.Wait()
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
