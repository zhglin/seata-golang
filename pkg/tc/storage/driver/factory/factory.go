package factory

import (
	"fmt"

	"github.com/opentrx/seata-golang/v2/pkg/tc/storage"
)

// driverFactories stores an internal mapping between storage driver names and their respective
// factories
// 存储驱动程序名称和它们各自的工厂之间的内部映射
var driverFactories = make(map[string]StorageDriverFactory)

// StorageDriverFactory is a factory interface for creating storage.StorageDriver interfaces
// Storage drivers should call Register() with a factory to make the driver available by name.
// Individual StorageDriver implementations generally register with the factory via the Register
// func (below) in their init() funcs, and as such they should be imported anonymously before use.
// 是用于创建存储的工厂接口。
// StorageDriver接口存储驱动程序应该使用一个工厂调用Register()以使驱动程序按名称可用。
// 单独的StorageDriver实现通常通过其init()函数中的register func(如下所示)向工厂注册，因此在使用之前应该匿名导入它们。
type StorageDriverFactory interface {
	// Create returns a new storage.StorageDriver with the given parameters
	// Parameters will vary by driver and may be ignored
	// Each parameter key must only consist of lowercase letters and numbers
	// 返回一个新的存储。
	// 带有给定参数的StorageDriver参数会因驱动而异，可能会被忽略。
	// 每个参数键只能由小写字母和数字组成
	Create(parameters map[string]interface{}) (storage.StorageDriver, error)
}

// Register makes a storage driver available by the provided name.
// If Register is called twice with the same name or if driver factory is nil, it panics.
// Additionally, it is not concurrency safe. Most Storage Drivers call this function
// in their init() functions. See the documentation for StorageDriverFactory for more.
// Register通过提供的名称使存储驱动程序可用。
// 如果Register以相同的名称被调用两次，或者driver factory为nil，它就会恐慌。
// 此外，它不是并发安全的。大多数存储驱动程序在它们的init()函数中调用这个函数。
func Register(name string, factory StorageDriverFactory) {
	if factory == nil {
		panic("Must not provide nil StorageDriverFactory")
	}
	_, registered := driverFactories[name]
	if registered { // 重复注册panic
		panic(fmt.Sprintf("StorageDriverFactory named %s already registered", name))
	}

	driverFactories[name] = factory
}

// Create a new storage.StorageDriver with the given name and
// parameters. To use a driver, the StorageDriverFactory must first be
// registered with the given name. If no drivers are found, an
// InvalidStorageDriverError is returned
// 创建一个新的存储。带有给定名称和参数的StorageDriver。
// 要使用驱动程序，必须首先用给定的名称注册StorageDriverFactory。
// 如果没有找到驱动程序，则返回InvalidStorageDriverError
func Create(name string, parameters map[string]interface{}) (storage.StorageDriver, error) {
	driverFactory, ok := driverFactories[name]
	if !ok {
		return nil, InvalidStorageDriverError{name}
	}
	return driverFactory.Create(parameters)
}

// InvalidStorageDriverError records an attempt to construct an unregistered storage driver
type InvalidStorageDriverError struct {
	Name string
}

func (err InvalidStorageDriverError) Error() string {
	return fmt.Sprintf("StorageDriver not registered: %s", err.Name)
}
