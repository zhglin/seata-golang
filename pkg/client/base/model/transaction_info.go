package model

import "fmt"

// Propagation transaction isolation level
// 事务隔离级别
// 上下游服务调用时的下游的全局事务行为
type Propagation byte

const (
	// Required 开启全局事务
	Required Propagation = iota

	// RequiresNew 开启新的全局事务
	RequiresNew

	// NotSupported 不支持全局事务，存在就解除
	NotSupported

	// Supports 支持，不解绑全局事务
	Supports

	// Never 完全不支持，存在全局事务报错，
	Never

	// Mandatory 存在全局事务报错，不存在开启
	Mandatory
)

// String
func (t Propagation) String() string {
	switch t {
	case Required:
		return "Required"
	case RequiresNew:
		return "REQUIRES_NEW"
	case NotSupported:
		return "NOT_SUPPORTED"
	case Supports:
		return "Supports"
	case Never:
		return "Never"
	case Mandatory:
		return "Mandatory"
	default:
		return fmt.Sprintf("%d", t)
	}
}

// TransactionInfo used to configure global transaction parameters
// 用于配置全局事务参数
type TransactionInfo struct {
	TimeOut     int32
	Name        string
	Propagation Propagation
}
