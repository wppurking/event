package work

import "reflect"

// 抽象出某一个类型的 Job, 代表了具体的 Name, 调用的方法以及相关的参数
type jobType struct {
	Name string
	JobOptions

	IsGeneric bool
	// GenericHandler 不需要动态初始化 Context 的最普通的方法
	GenericHandler GenericHandler
	DynamicHandler reflect.Value
}

// GenericHandler is a job handler without any custom context.
type GenericHandler func(*Job) error
