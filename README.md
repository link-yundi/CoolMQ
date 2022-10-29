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

var bus = NewMqBus()

func main() {
    task1 := "task1"
    task2 := "task2"
    task3 := "task3"
    // ========================== 添加主题 ==========================
    AddTopic(bus, task1, 100, 100, handler1, task1Close) // 通过producerLimit以及consumerLimit控制任务效率
    AddTopic(bus, task2, 100, 100, handler2, nil)
    AddTopic(bus, task3, 100, 100, handler3, nil)
    // ========================== 控制整体并发 ==========================
    SetProducerLimit(bus, 1300)
    // ========================== 启动 ==========================
    for i := 0; i < 100; i++ {
        // ========================== task1新增子任务 ==========================
        for a := 0; a < 10; a++ {
            topic1 := fmt.Sprintf("%d_%d", i, a)
            AddTopic(bus, topic1, 1, 1, handler1, nil)
            Produce(bus, task1, i)
            go Close(bus, topic1) // 可交由协程也可堵塞关闭
        }
        Produce(bus, task2, i)
        Produce(bus, task3, i)
    }
    // 交由后台等待任务完成关闭
    go Close(bus, task1, task2, task3)
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
```

