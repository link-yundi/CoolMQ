# CoolMQ

### 安装

```sh
go get -u github.com/link-yundi/CoolMQ
```

### 特性

- 多topic, 每个topic多个消费者以及多个生产者，消费与生产分离

- 单独控制每个topic的速度
- 控制整体的速度

### 示例

```go

func main() {
	task1 := "task1"
	task2 := "task2"
	task3 := "task3"
	// ========================== 添加主题 ==========================
	CoolMQ.AddTopic(task1, 100, 100, handler1) // 控制task1的速度
	CoolMQ.AddTopic(task2, 100, 20, handler2) 
	CoolMQ.AddTopic(task3, 100, 100, handler3)
	// ========================== 控制整体并发 ==========================
	CoolMQ.SetProducerLimit(300)
	// ========================== 启动 ==========================
	CoolMQ.Work()
	for i := 0; i < 100; i++ {
		log.Debug(i)
		// ========================== 生产数据 ==========================
		CoolMQ.Produce(task1, i)
		CoolMQ.Produce(task2, i)
		CoolMQ.Produce(task3, i)
	}
	// ========================== 堵塞等待 ==========================
	CoolMQ.Wait()
	CoolMQ.Close()
}

func handler1(msg *Msg) {
	fmt.Println(msg.Topic, msg.Data.(int))
	time.Sleep(1 * time.Second)
	CoolMQ.Done(msg.Topic)
}

func handler2(msg *Msg) {
	fmt.Println(msg.Topic, msg.Data.(int))
	time.Sleep(10 * time.Second)
	CoolMQ.Done(msg.Topic)
}

func handler3(msg *Msg) {
	fmt.Println(msg.Topic, msg.Data.(int))
	time.Sleep(5 * time.Second)
	CoolMQ.Done(msg.Topic)
}
```

