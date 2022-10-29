# CoolMQ

### 安装

```sh
go get -u github.com/link-yundi/coolmq
```

### 特性

- 多topic, 每个topic多个消费者以及多个生产者，消费与生产分离
- 单独控制每个topic的速度
- 控制整体的速度
- topic 完成通知，触发自定义后续处理

### 示例

```go
func main() {
    task1 := "task1"
    task2 := "task2"
    task3 := "task3"
    // ========================== 添加主题 ==========================
    coolmq.AddTopic(task1, 100, 100, handler1, task1Close) // 通过producerLimit以及consumerLimit控制任务效率
    coolmq.AddTopic(task2, 100, 100, handler2, nil)
    coolmq.AddTopic(task3, 100, 100, handler3, nil)
    // ========================== 控制整体并发 ==========================
    coolmq.SetProducerLimit(1300)
    // ========================== 启动 ==========================
    for i := 0; i < 100; i++ {
    // ========================== task1新增子任务 ==========================
        for a := 0; a < 10; a++ {
            topic1 := fmt.Sprintf("%d_%d", i, a)
            coolmq.AddTopic(topic1, 1, 1, handler1, nil)
            coolmq.Produce(task1, i)
            go coolmq.Close(topic1) // 可交由协程也可堵塞关闭
        }
        coolmq.Produce(task2, i)
        coolmq.Produce(task3, i)
    }
    // 交由后台等待任务完成关闭
	go coolmq.Close(task1, task2, task3)
    // 堵塞等待所有topic完成
    coolmq.Wait()
}

func handler1(msg *Msg) {
	fmt.Println(msg.Topic, msg.Data.(int))
	time.Sleep(1 * time.Second)
	coolmq.Done(msg.Topic)
}

func handler2(msg *Msg) {
	fmt.Println(msg.Topic, msg.Data.(int))
	time.Sleep(10 * time.Second)
	coolmq.Done(msg.Topic)
}

func handler3(msg *Msg) {
	fmt.Println(msg.Topic, msg.Data.(int))
	time.Sleep(5 * time.Second)
	coolmq.Done(msg.Topic)
}

func task1Close(topic string) {
    log.Info(topic, "CLOSE!")
}
```

