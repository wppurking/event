package work

import (
	"reflect"
	"sync/atomic"
)

// 抽象出某一个类型的 Message, 代表了具体的 Name, 调用的方法以及相关的参数
type consumerType struct {
	Name string
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
