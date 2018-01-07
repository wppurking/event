package work

// JobOptions can be passed to JobWithOptions.
type JobOptions struct {
	RoutingKey     string            // 给 RabbitMQ 使用的 routing_key, 如果存在则绑定, 如果不存在则只 consume queue
	Prefetch       int               // 指定队列的 Prefetch 数量
	Priority       uint              // Priority from 1 to 10000
	MaxFails       uint              // 1: send straight to dead (unless SkipDead)
	SkipDead       bool              // If true, don't send failed jobs to the dead queue when retries are exhausted.
	MaxConcurrency uint              // Max number of jobs to keep in flight (default is 0, meaning no max)
	Backoff        BackoffCalculator // If not set, uses the default backoff algorithm
}
