package model

import (
	"github.com/opentrx/seata-golang/v2/pkg/apis"
	"github.com/opentrx/seata-golang/v2/pkg/util/time"
)

type GlobalTransaction struct {
	*apis.GlobalSession // 全局事务

	BranchSessions map[*apis.BranchSession]bool // 分支事务
}

func (gt *GlobalTransaction) Add(branchSession *apis.BranchSession) {
	branchSession.Status = apis.Registered
	gt.BranchSessions[branchSession] = true
}

func (gt *GlobalTransaction) Remove(branchSession *apis.BranchSession) {
	delete(gt.BranchSessions, branchSession)
}

// GetBranch 查询指定的分支事务
func (gt *GlobalTransaction) GetBranch(branchID int64) *apis.BranchSession {
	for branchSession := range gt.BranchSessions {
		if branchSession.BranchID == branchID {
			return branchSession
		}
	}
	return nil
}

// CanBeCommittedAsync 是否能异步提交
func (gt *GlobalTransaction) CanBeCommittedAsync() bool {
	for branchSession := range gt.BranchSessions {
		if branchSession.Type == apis.TCC {
			return false
		}
	}
	return true
}

// IsSaga 是否是saga模式
func (gt *GlobalTransaction) IsSaga() bool {
	for branchSession := range gt.BranchSessions {
		if branchSession.Type == apis.SAGA {
			return true
		}
	}
	return false
}

func (gt *GlobalTransaction) IsTimeout() bool {
	return (time.CurrentTimeMillis() - uint64(gt.BeginTime)) > uint64(gt.Timeout)
}

func (gt *GlobalTransaction) IsRollingBackDead() bool {
	return (time.CurrentTimeMillis() - uint64(gt.BeginTime)) > uint64(2*6000)
}

func (gt *GlobalTransaction) IsTimeoutGlobalStatus() bool {
	return gt.Status == apis.TimeoutRolledBack ||
		gt.Status == apis.TimeoutRollbackFailed ||
		gt.Status == apis.TimeoutRollingBack ||
		gt.Status == apis.TimeoutRollbackRetrying
}

// HasBranch 是否存在分支事务
func (gt *GlobalTransaction) HasBranch() bool {
	return len(gt.BranchSessions) > 0
}

// Begin 开启全局事务 设置状态
func (gt *GlobalTransaction) Begin() {
	gt.Status = apis.Begin
	gt.BeginTime = int64(time.CurrentTimeMillis())
	gt.Active = true
}
