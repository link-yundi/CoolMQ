package coolmq

import (
	"fmt"
	log "github.com/link-yundi/ylog"
	"testing"
	"time"
)

/**
------------------------------------------------
Created on 2022-10-27 16:40
@Author: ZhangYundi
@Email: yundi.xxii@outlook.com
------------------------------------------------
**/

var bus = NewMqBus()

func TestMQ(t *testing.T) {
	task1 := "task1"
	task2 := "task2"
	task3 := "task3"
	// ========================== 添加主题 ==========================
	AddTopic(bus, &MqConfig{
		Topic:         task1,
		ProducerLimit: 150,
		ConsumerLimit: 150,
		Consumer:      handler1,
		CloseTrigger:  task1Close,
	}) // 通过producerLimit以及consumerLimit控制任务效率
	AddTopic(bus, &MqConfig{
		Topic:         task2,
		ProducerLimit: 150,
		ConsumerLimit: 150,
		Consumer:      handler2,
		CloseTrigger:  nil,
	})
	AddTopic(bus, &MqConfig{
		Topic:         task3,
		ProducerLimit: 150,
		ConsumerLimit: 150,
		Consumer:      nil,
		CloseTrigger:  nil,
	})
	// ========================== 启动 ==========================
	for i := 0; i < 100; i++ {
		// ========================== task1新增子任务 ==========================
		topic1 := fmt.Sprintf("%d", i)
		AddTopic(bus, &MqConfig{
			Topic:         topic1,
			ProducerLimit: 100,
			ConsumerLimit: 100,
			Consumer:      handler2, // 如果没有并发，消费一个需要10s
			CloseTrigger:  nil,
		})
		for a := 0; a < 10; a++ {
			Produce(bus, topic1, a)
		}
		Produce(bus, task1, i)
		Produce(bus, task2, i)
		Produce(bus, task3, i)
		Stop(bus, topic1)
	}
	// 交由后台等待任务完成关闭
	Stop(bus, task1, task2, task3)
	// 堵塞等待所有topic完成
	Wait(bus)
}

func handler1(msg *Msg) {
	log.Debug(msg.Topic, msg.Data.(int))
	time.Sleep(1 * time.Second)
	Done(bus, msg.Topic)
}

func task1Close(topic string) {
	log.Info(topic, "CLOSE!")
}

func handler2(msg *Msg) {
	//fmt.Println(msg.Topic, msg.Data.(int))
	time.Sleep(10 * time.Second)
	Done(bus, msg.Topic)
}

func handler3(msg *Msg) {
	//fmt.Println(msg.Topic, msg.Data.(int))
	time.Sleep(5 * time.Second)
	Done(bus, msg.Topic)
}
