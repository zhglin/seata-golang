package server

import (
	"time"

	"github.com/opentrx/seata-golang/v2/pkg/apis"
)

type GlobalSessionLocker interface {
	TryLock(session *apis.GlobalSession, timeout time.Duration) (bool, error)

	Unlock(session *apis.GlobalSession)
}

// UnimplementedGlobalSessionLocker 未实现的全局锁
type UnimplementedGlobalSessionLocker struct {
}

func (locker *UnimplementedGlobalSessionLocker) TryLock(session *apis.GlobalSession, timeout time.Duration) (bool, error) {
	return true, nil
}

func (locker *UnimplementedGlobalSessionLocker) Unlock(session *apis.GlobalSession) {

}
