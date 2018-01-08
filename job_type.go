package work

import (
	"reflect"
	"sync"
)

// 抽象出某一个类型的 Job, 代表了具体的 Name, 调用的方法以及相关的参数
type jobType struct {
	Name string
	JobOptions

	IsGeneric      bool
	GenericHandler GenericHandler // GenericHandler 不需要动态初始化 Context 的最普通的方法
	DynamicHandler reflect.Value
	runs           uint // Running 正在运行的 jobs
	mtx            sync.Mutex
}

func (jt *jobType) incr() {
	jt.mtx.Lock()
	jt.runs++
	jt.mtx.Unlock()
}

func (jt *jobType) decr() {
	jt.mtx.Lock()
	jt.runs--
	jt.mtx.Unlock()
}

// GenericHandler is a job handler without any custom context.
type GenericHandler func(*Job) error
