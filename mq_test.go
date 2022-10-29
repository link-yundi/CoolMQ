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

func TestMQ(t *testing.T) {
	task1 := "task1"
	task2 := "task2"
	task3 := "task3"
	// ========================== 添加主题 ==========================
	AddTopic(task1, 100, 100, handler1, task1Close) // 通过producerLimit以及consumerLimit控制任务效率
	AddTopic(task2, 100, 100, handler2, nil)
	AddTopic(task3, 100, 100, handler3, nil)
	// ========================== 控制整体并发 ==========================
	SetProducerLimit(1300)
	// ========================== 启动 ==========================
	for i := 0; i < 100; i++ {
		// ========================== task1新增子任务 ==========================
		for a := 0; a < 10; a++ {
			topic1 := fmt.Sprintf("%d_%d", i, a)
			AddTopic(topic1, 1, 1, handler1, nil)
			Produce(task1, i)
			Close(topic1)
		}
		Produce(task2, i)
		Produce(task3, i)
	}
	// 交由后台等待任务完成关闭
	Close(task1)
	Close(task2)
	Close(task3)
	// 堵塞等待所有topic完成
	Wait()
}

func handler1(msg *Msg) {
	log.Debug(msg.Topic, msg.Data.(int))
	time.Sleep(1 * time.Second)
	Done(msg.Topic)
}

func task1Close(topic string) {
	log.Info(topic, "CLOSE!")
}

func handler2(msg *Msg) {
	//fmt.Println(msg.Topic, msg.Data.(int))
	time.Sleep(10 * time.Second)
	Done(msg.Topic)
}

func handler3(msg *Msg) {
	//fmt.Println(msg.Topic, msg.Data.(int))
	time.Sleep(5 * time.Second)
	Done(msg.Topic)
}
