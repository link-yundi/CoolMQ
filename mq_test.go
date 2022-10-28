package coolmq

import (
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
	AddTopic(task1, 100, 100, handler1) // 通过producerLimit以及consumerLimit控制任务效率
	AddTopic(task2, 20, 100, handler2)
	AddTopic(task3, 100, 100, handler3)
	// ========================== 控制整体并发 ==========================
	SetProducerLimit(300)
	// ========================== 启动 ==========================
	Work()
	for i := 0; i < 100; i++ {
		// ========================== 生产数据 ==========================
		Produce(task1, i)
		Produce(task2, i)
		Produce(task3, i)
	}
	Close() // 先完成的先关闭释放
}

func handler1(msg *Msg) {
	//fmt.Println(msg.Topic, msg.Data.(int))
	time.Sleep(1 * time.Second)
	Done(msg.Topic)
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
