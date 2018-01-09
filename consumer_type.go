package work

import (
	"reflect"
	"sync/atomic"
)

// consumerType 代表具体某一个 Consumer 需要执行的方法抽象.
// RoutingKey 为具体的 RoutingKey, 调用的方法以及相关的参数
type consumerType struct {
	RoutingKey string
	ConsumerOptions

	IsGeneric      bool
	GenericHandler GenericHandler // GenericHandler 不需要动态初始化 Context 的最普通的方法
	DynamicHandler reflect.Value
	runs           uint32 // Running 正在运行的 jobs
}

func (ct *consumerType) incr() {
	atomic.AddUint32(&ct.runs, 1)
}

func (ct *consumerType) decr() {
	n := -1
	atomic.AddUint32(&ct.runs, uint32(n))
}

func (ct *consumerType) Runs() uint {
	return uint(ct.runs)
}

// GenericHandler is a job handler without any custom context.
type GenericHandler func(*Message) error
