package coolmq

import (
	log "github.com/link-yundi/ylog"
	"strconv"
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
	SetProducerLimit(1300)
	// ========================== 启动 ==========================
	for i := 0; i < 100; i++ {
		// ========================== task1新增子任务 ==========================
		for a := 0; a < 10; a++ {
			topic1 := strconv.FormatInt(int64(a), 10)
			AddTopic(topic1, 10, 10, handler1)
			Produce(task1, i)
		}
		Produce(task2, i)
		Produce(task3, i)
	}

	Close() // 先完成的先关闭释放
}

func handler1(msg *Msg) {
	log.Debug(msg.Topic, msg.Data.(int))
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
