# wppurking/event [![GoDoc](https://godoc.org/github.com/wppurking/event?status.png)](https://godoc.org/github.com/wppurking/event)

wppurking/event 被设计为针对后端 RabbitMQ 的 Event 消息处理框架. 虽然可以涵盖部分 background job 的范畴, 
但其核心为尽可能的利用 RabbitMQ 本身的特性用于处理 JSON 格式的 Message 消息. 代码 fork 自 [gocraft/work](https://github.com/gocraft/work),
其设计与 Ruby 环境的 [Hutch](https://github.com/gocardless/hutch) 与 [Hutch-schedule](https://github.com/wppurking/hutch-schedule) 进行合作沟通.

## Tips
### 后端任务需要的功能
1. 某种后端消息格式, 通过一稳定后端中间件进行分布式的任务处理
2. 可以设置延迟任务
3. 可以设置唯一任务
4. 可以设置 cron 任务
5. 可以进行总并发量控制, 同时可以在总并发量之下, 控制单个任务并发
6. 任务错误重试
7. 重试过多后, 可查看死亡任务, 或者可以手动重新投递
8. 拥有一个 UI 界面可查看内部执行情况
9. 任务可以拥有优先级

### Event 的涵盖的功能
1. 通过 RabbitMQ 以及纯粹自定义的 JSON Format message, 进行分布式任务处理
2. 可以设置延迟的 event 消息处理, 由 RabbitMQ 负责(fixed delay level)
3. 可以进行总并发量控制, 同时可以在总并发量之下, 控制单个 Consumer 的并发
4. Event 消息处理失败后重试. RabbitMQ 负责信息记录
5. Event 消息失败过多后进入 Dead 队列. (非 RabbitMQ 的 DLX, 仅仅是 Dead 队列)
6. 利用 RabbitMQ 的 UI 面板查看任务处理情况以及速度
7. 默认与 [Hutch-schedule](https://github.com/wppurking/hutch-schedule) 兼容, 做到 Ruby/Golang 两端消息可相互传递消息各自执行

### 现在的开源的后端任务项目介绍
下面列举了不同的后端任务/消息处理的框架, 可以根据需要进行选择
 
| 产品 | 后端 | 描述 |
| --- | --- | --- |
| [gocraft/work](https://github.com/gocraft/work) | redis | golang 独立运行. 功能完备 |
| [go-workers](https://github.com/jrallison/go-workers) | redis | golang 运行, 兼容 Sidekiq 消息格式 |
| [iamduo/workq](https://github.com/iamduo/workq)| self server | golang 运行, 独立的后端任务系统 |
| [gocelery](https://github.com/gocelery/gocelery) | redis, rabbitmq | golang 运行, 兼容 Celery 消息格式 |
| [hutch](https://github.com/gocardless/hutch) | rabbitmq | ruby 运行, *纯粹 json 消息格式* |
| [wppurking/event](https://github.com/wppurking/event) | rabbitmq | golang 运行, *纯粹 json 消息格式* |


## Enqueue new jobs
TBD

## Process jobs
TBD

## Special Features

### Contexts

Just like in [gocraft/web](https://www.github.com/gocraft/web), gocraft/work lets you use your own contexts. Your context can be empty or it can have various fields in it. The fields can be whatever you want - it's your type! When a new job is processed by a worker, we'll allocate an instance of this struct and pass it to your middleware and handlers. This allows you to pass information from one middleware function to the next, and onto your handlers.

Custom contexts aren't really needed for trivial example applications, but are very important for production apps. For instance, one field in your context can be your tagged logger. Your tagged logger augments your log statements with a job-id. This lets you filter your logs by that job-id.

### Scheduled Jobs

You can schedule jobs to be executed in the future. To do so, make a new ```Enqueuer``` and call its ```EnqueueIn``` method:

```go
publisher := event.NewPublisher("my_app_namespace", cony.URL(*rabbitMqURL))
secondsInTheFuture := 300
_, err := publisher.PublishIn("app.prod.send_welcome_email", secondsInTheFuture, event.Q{"address": "test@example.com"})
```

### Unique Jobs
当前设计为 Event 的消息处理框架, 而不是后端任务, 同时 RabbitMQ 在集群环境下也无法保证消息的唯一. 所以:
1. 如果需要消息唯一, 请在用户端进行处理. 例如将状态记录在 DB.
2. 将 event 的消息处理设计成为等幂处理, 多次执行得到同样的结果.

## Design and concepts

### Enqueueing jobs

* When jobs are enqueued, they're serialized with JSON and added to a simple Redis list with LPUSH.
* Jobs are added to a list with the same name as the job. Each job name gets its own queue. Whereas with other job systems you have to design which jobs go on which queues, there's no need for that here.

### Processing a job

* To process a job, a worker will execute a Lua script to atomically move a job its queue to an in-progress queue.
  * A job is dequeued and moved to in-progress if the job queue is not paused and the number of active jobs does not exceed concurrency limit for the job type 
* The worker will then run the job and increment the job lock. The job will either finish successfully or result in an error or panic.
  * If the process completely crashes, the reaper will eventually find it in its in-progress queue and requeue it.
* If the job is successful, we'll simply remove the job from the in-progress queue.
* If the job returns an error or panic, we'll see how many retries a job has left. If it doesn't have any, we'll move it to the dead queue. If it has retries left, we'll consume a retry and add the job to the retry queue. 

### Workers and WorkerPools

* WorkerPools provide the public API of gocraft/work.
  * You can attach jobs and middleware to them.
  * You can start and stop them.
  * Based on their concurrency setting, they'll spin up N worker goroutines.
* Each worker is run in a goroutine. It will get a job from redis, run it, get the next job, etc.
  * Each worker is independent. They are not dispatched work -- they get their own work.

### Retry job, scheduled jobs, and the requeuer

* In addition to the normal list-based queues that normal jobs live in, there are two other types of queues: the retry queue and the scheduled job queue.
* Both of these are implemented as Redis z-sets. The score is the unix timestamp when the job should be run. The value is the bytes of the job.
* The requeuer will occasionally look for jobs in these queues that should be run now. If they should be, they'll be atomically moved to the normal list-based queue and eventually processed.

### Dead jobs

* 当 Message 被重新尝试超过一定数量后, 将会被投递到 Dead Queue 中
* Dead Queue 是 RabbitMQ 中一个有限长度的 Queue 以及有 TTL 长的 Queue, 两种任意一个到达都将会清理 Dead Queue
* 想查看或者重新投递, 通过 RabbitMQ 的 management UI

## Consumer 级别的并发控制

* 你可以通过 `ConsumerOptions{MaxConcurrency: <num>}` 来控制并发, 同时也注意 RabbitMQ 中的 `Prefetch`
* 与 WorkerPool 的并发量不同, 这个用于并发在单个 redis 实例中, 有多少个 gorouting 可以同时运行(最多不超过 WorkerPool 的总量)
* 用于控制任务并发的参数记录在为每一个 Consumer 对应的 `consumerType.run` 上
* 默认的控制并发为 `0`, 意味着 `当前 consume 没有限制`
* **注意** 如果你想设置 consumer 为 "单线程" 那么你可以设置 `MaxConcurrency` 如下:
```go
      worker_pool.ConsumerWithOptions(routingKey, ConsumerOptions{Prefetch: 30, MaxConcurrency: 5}, (*Context).WorkFxn)
```

## Benchmarks

The benches folder contains various benchmark code. In each case, we enqueue 100k jobs across 5 queues. The jobs are almost no-op jobs: they simply increment an atomic counter. We then measure the rate of change of the counter to obtain our measurement.

| Library | Speed |
| --- | --- |
| [gocraft/work](https://www.github.com/gocraft/work)/[wppurking/event](https://github.com/wppurking/event) | **20944 jobs/s** |
| [jrallison/go-workers](https://www.github.com/jrallison/go-workers) | 19945 jobs/s |
| [benmanns/goworker](https://www.github.com/benmanns/goworker) | 10328.5 jobs/s |
| [albrow/jobs](https://www.github.com/albrow/jobs) | 40 jobs/s |


## Authors

* Jonathan Novak -- [https://github.com/cypriss](https://github.com/cypriss)
* Tai-Lin Chu -- [https://github.com/taylorchu](https://github.com/taylorchu)
* Sponsored by [UserVoice](https://eng.uservoice.com)
* Wyatt Pan [https://github.com/wppurking](https://github.com/wppurking)
