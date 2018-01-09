package work

import (
	"reflect"
	"sync/atomic"
)

// 抽象出某一个类型的 Message, 代表了具体的 Name, 调用的方法以及相关的参数
type jobType struct {
	Name string
	JobOptions

	IsGeneric      bool
	GenericHandler GenericHandler // GenericHandler 不需要动态初始化 Context 的最普通的方法
	DynamicHandler reflect.Value
	runs           uint32 // Running 正在运行的 jobs
}

func (jt *jobType) incr() {
	atomic.AddUint32(&jt.runs, 1)
}

func (jt *jobType) decr() {
	n := -1
	atomic.AddUint32(&jt.runs, uint32(n))
}

func (jt *jobType) Runs() uint {
	return uint(jt.runs)
}

// GenericHandler is a job handler without any custom context.
type GenericHandler func(*Message) error
