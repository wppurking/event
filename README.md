# wppurking/event [![GoDoc](https://godoc.org/github.com/wppurking/event?status.png)](https://godoc.org/github.com/wppurking/event)

wppurking/event 被设计为针对后端 RabbitMQ 的 Event 消息处理框架. 虽然可以涵盖部分 background job 的范畴, 但其核心为
尽可能的利用 RabbitMQ 本身的特性用于处理 JSON 格式的 Message 消息. 代码 fork 自 [gocraft/work](https://github.com/gocraft/work), 并且受到
 [Hutch](https://github.com/gocardless/hutch) 与 [Hutch-schedule](https://github.com/wppurking/hutch-schedule) 的影响.

* Fast and efficient. Faster than [this](https://www.github.com/jrallison/go-workers), [this](https://www.github.com/benmanns/goworker), and [this](https://www.github.com/albrow/jobs). See below for benchmarks.
* Reliable - don't lose event even if your process crashes. [rabbitmq ack, dlx, ttl]
* Middleware on events -- good for metrics instrumentation, logging, etc.
* If a event fails, it will be retried a specified number of times.
* Schedule event to happen in the future.
* Control concurrency within and across processes

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
2. 可以设置延迟的 event 消息处理, 由 RabbitMQ 负责.
3. 可以进行总并发量控制, 同时可以在总并发量之下, 控制单个 Consumer 的并发
4. Event 消息处理失败后重试. RabbitMQ 负责信息记录.
5. Event 消息失败过多后进入 Dead 队列. (非 RabbitMQ 的 DLX, 仅仅是 Dead 队列)
6. 利用 RabbitMQ 的 UI 面板查看任务处理情况以及速度
7. 利用 RabbitMQ 的特性实现不同 Event 的优先级. (暂未实现)
8. 默认与 [hutch](https://github.com/gocardless/hutch) 兼容, 可无区别消费 Event

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

### Check-ins
无内容

### Scheduled Jobs

You can schedule jobs to be executed in the future. To do so, make a new ```Enqueuer``` and call its ```EnqueueIn``` method:

```go
enqueuer := work.NewEnqueuer("my_app_namespace", redisPool)
secondsInTheFuture := 300
_, err := enqueuer.EnqueueIn("send_welcome_email", secondsInTheFuture, work.Q{"address": "test@example.com"})
```

### Unique Jobs
当前设计为 Event 的消息处理框架, 而不是后端任务, 同时 RabbitMQ 在集群环境下也无法保证消息的唯一. 所以:
1. 如果需要消息唯一, 请在用户端进行处理. 例如将状态记录在 DB.
2. 将 event 的消息处理设计成为等幂处理, 多次执行得到同样的结果.

### Periodic Enqueueing (Cron)
当前设计为 Event 的消息处理框架, 而不是后端任务, 所以取消 cron 任务注册

## Design and concepts

### Enqueueing jobs

* When jobs are enqueued, they're serialized with JSON and added to a simple Redis list with LPUSH.
* Jobs are added to a list with the same name as the job. Each job name gets its own queue. Whereas with other job systems you have to design which jobs go on which queues, there's no need for that here.

### Scheduling algorithm

* Each job lives in a list-based queue with the same name as the job.
* Each of these queues can have an associated priority. The priority is a number from 1 to 100000.
* Each time a worker pulls a job, it needs to choose a queue. It chooses a queue probabilistically based on its relative priority.
* If the sum of priorities among all queues is 1000, and one queue has priority 100, jobs will be pulled from that queue 10% of the time.
* Obviously if a queue is empty, it won't be considered.
* The semantics of "always process X jobs before Y jobs" can be accurately approximated by giving X a large number (like 10000) and Y a small number (like 1).

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

* After a job has failed a specified number of times, it will be added to the dead job queue.
* The dead job queue is just a Redis z-set. The score is the timestamp it failed and the value is the job.
* To retry failed jobs, use the UI or the Client API.

### The reaper

* If a process crashes hard (eg, the power on the server turns off or the kernal freezes), some jobs may be in progress and we won't want to lose them. They're safe in their in-progress queue.
* The reaper will look for worker pools without a heartbeat. It will scan their in-progress queues and requeue anything it finds.

## Job concurrency

* You can control job concurrency using `JobOptions{MaxConcurrency: <num>}`.
* Unlike the WorkerPool concurrency, this controls the limit on the number jobs of that type that can be active at one time by within a single redis instance
* This works by putting a precondition on enqueuing function, meaning a new job will not be scheduled if we are at or over a job's `MaxConcurrency` limit
* A redis key (see `redisKeyJobsLock`) is used as a counting semaphore in order to track job concurrency per job type
* The default value is `0`, which means "no limit on job concurrency"
* **Note:** if you want to run jobs "single threaded" then you can set the `MaxConcurrency` accordingly:
```go
      worker_pool.JobWithOptions(jobName, JobOptions{MaxConcurrency: 1}, (*Context).WorkFxn)
```

### Terminology reference
* "worker pool" - a pool of workers
* "worker" - an individual worker in a single goroutine. Gets a job from redis, does job, gets next job...
* "heartbeater" or "worker pool heartbeater" - goroutine owned by worker pool that runs concurrently with workers. Writes the worker pool's config/status (aka "heartbeat") every 5 seconds.
* "heartbeat" - the status written by the heartbeater.
* "observer" or "worker observer" - observes a worker. Writes stats. makes "observations".
* "worker observation" - A snapshot made by an observer of what a worker is working on.
* "periodic enqueuer" - A process that runs with a worker pool that periodically enqueues new jobs based on cron schedules.
* "job" - the actual bundle of data that constitutes one job
* "job name" - each job has a name, like "create_watch"
* "job type" - backend/private nomenclature for the handler+options for processing a job
* "queue" - each job creates a queue with the same name as the job. only jobs named X go into the X queue.
* "retry jobs" - if a job fails and needs to be retried, it will be put on this queue.
* "scheduled jobs" - jobs enqueued to be run in th future will be put on a scheduled job queue.
* "dead jobs" - if a job exceeds its MaxFails count, it will be put on the dead job queue.
* "paused jobs" - if paused key is present for a queue, then no jobs from that queue will be processed by any workers until that queue's paused key is removed
* "job concurrency" - the number of jobs being actively processed  of a particular type across worker pool processes but within a single redis instance

## Benchmarks

The benches folder contains various benchmark code. In each case, we enqueue 100k jobs across 5 queues. The jobs are almost no-op jobs: they simply increment an atomic counter. We then measure the rate of change of the counter to obtain our measurement.

| Library | Speed |
| --- | --- |
| [gocraft/work](https://www.github.com/gocraft/work) | **20944 jobs/s** |
| [jrallison/go-workers](https://www.github.com/jrallison/go-workers) | 19945 jobs/s |
| [benmanns/goworker](https://www.github.com/benmanns/goworker) | 10328.5 jobs/s |
| [albrow/jobs](https://www.github.com/albrow/jobs) | 40 jobs/s |


## gocraft

gocraft offers a toolkit for building web apps. Currently these packages are available:

* [gocraft/web](https://github.com/gocraft/web) - Go Router + Middleware. Your Contexts.
* [gocraft/dbr](https://github.com/gocraft/dbr) - Additions to Go's database/sql for super fast performance and convenience.
* [gocraft/health](https://github.com/gocraft/health) - Instrument your web apps with logging and metrics.
* [gocraft/work](https://github.com/gocraft/work) - Process background jobs in Go.

These packages were developed by the [engineering team](https://eng.uservoice.com) at [UserVoice](https://www.uservoice.com) and currently power much of its infrastructure and tech stack.

## Authors

* Jonathan Novak -- [https://github.com/cypriss](https://github.com/cypriss)
* Tai-Lin Chu -- [https://github.com/taylorchu](https://github.com/taylorchu)
* Sponsored by [UserVoice](https://eng.uservoice.com)
* Wyatt Pan [https://github.com/wppurking](https://github.com/wppurking)
