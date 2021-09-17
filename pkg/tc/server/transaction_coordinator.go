package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/gogo/protobuf/types"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/opentrx/seata-golang/v2/pkg/apis"
	common2 "github.com/opentrx/seata-golang/v2/pkg/common"
	"github.com/opentrx/seata-golang/v2/pkg/tc/config"
	"github.com/opentrx/seata-golang/v2/pkg/tc/event"
	"github.com/opentrx/seata-golang/v2/pkg/tc/holder"
	"github.com/opentrx/seata-golang/v2/pkg/tc/lock"
	"github.com/opentrx/seata-golang/v2/pkg/tc/model"
	"github.com/opentrx/seata-golang/v2/pkg/tc/storage/driver/factory"
	"github.com/opentrx/seata-golang/v2/pkg/util/common"
	"github.com/opentrx/seata-golang/v2/pkg/util/log"
	"github.com/opentrx/seata-golang/v2/pkg/util/runtime"
	time2 "github.com/opentrx/seata-golang/v2/pkg/util/time"
	"github.com/opentrx/seata-golang/v2/pkg/util/uuid"
)

const AlwaysRetryBoundary = 0

// TransactionCoordinator 协调器 TC
type TransactionCoordinator struct {
	sync.Mutex
	maxCommitRetryTimeout            int64
	maxRollbackRetryTimeout          int64
	rollbackRetryTimeoutUnlockEnable bool

	asyncCommittingRetryPeriod time.Duration
	committingRetryPeriod      time.Duration
	rollingBackRetryPeriod     time.Duration
	timeoutRetryPeriod         time.Duration // 超时未提交的事务处理间隔

	streamMessageTimeout time.Duration

	holder             *holder.SessionHolder // 事务信息存储
	resourceDataLocker *lock.LockManager     // 行级锁管理器
	locker             GlobalSessionLocker   // 全局事务锁 未实现

	idGenerator        *atomic.Uint64 // 分支事务id生成器
	futures            *sync.Map      // callBackMessages中的id对应的响应
	activeApplications *sync.Map      // 已经建立链接的分支事务客户端 Server-side的rpc链接
	callBackMessages   *sync.Map      // 客户端address对应的queue
}

func NewTransactionCoordinator(conf *config.Configuration) *TransactionCoordinator {
	driver, err := factory.Create(conf.Storage.Type(), conf.Storage.Parameters())
	if err != nil {
		log.Fatalf("failed to construct %s driver: %v", conf.Storage.Type(), err)
		os.Exit(1)
	}
	tc := &TransactionCoordinator{
		maxCommitRetryTimeout:            conf.Server.MaxCommitRetryTimeout,
		maxRollbackRetryTimeout:          conf.Server.MaxRollbackRetryTimeout,
		rollbackRetryTimeoutUnlockEnable: conf.Server.RollbackRetryTimeoutUnlockEnable,

		asyncCommittingRetryPeriod: conf.Server.AsyncCommittingRetryPeriod,
		committingRetryPeriod:      conf.Server.CommittingRetryPeriod,
		rollingBackRetryPeriod:     conf.Server.RollingBackRetryPeriod,
		timeoutRetryPeriod:         conf.Server.TimeoutRetryPeriod,

		streamMessageTimeout: conf.Server.StreamMessageTimeout,

		holder:             holder.NewSessionHolder(driver),
		resourceDataLocker: lock.NewLockManager(driver),
		locker:             new(UnimplementedGlobalSessionLocker),

		idGenerator:        &atomic.Uint64{},
		futures:            &sync.Map{},
		activeApplications: &sync.Map{},
		callBackMessages:   &sync.Map{},
	}
	go tc.processTimeoutCheck()    // 超时未提交的全局事务 设置成回滚状态
	go tc.processAsyncCommitting() // 异步提交的全局事务
	go tc.processRetryCommitting() // 对CommitRetrying状态的事务进行提交
	go tc.processRetryRollingBack()

	return tc
}

// Begin 开启全局事务
func (tc *TransactionCoordinator) Begin(ctx context.Context, request *apis.GlobalBeginRequest) (*apis.GlobalBeginResponse, error) {
	transactionID := uuid.NextID()
	// 生成xid 返回客户端
	xid := common.GenerateXID(request.Addressing, transactionID)
	gt := model.GlobalTransaction{
		GlobalSession: &apis.GlobalSession{
			Addressing:      request.Addressing,
			XID:             xid,
			TransactionID:   transactionID,
			TransactionName: request.TransactionName,
			Timeout:         request.Timeout,
		},
	}

	// 设置全局事务状态
	gt.Begin()
	err := tc.holder.AddGlobalSession(gt.GlobalSession)
	if err != nil {
		return &apis.GlobalBeginResponse{
			ResultCode:    apis.ResultCodeFailed,
			ExceptionCode: apis.BeginFailed,
			Message:       err.Error(),
		}, nil
	}

	// 监控
	runtime.GoWithRecover(func() {
		evt := event.NewGlobalTransactionEvent(gt.TransactionID, event.RoleTC, gt.TransactionName, gt.BeginTime, 0, gt.Status)
		event.EventBus.GlobalTransactionEventChannel <- evt
	}, nil)

	log.Infof("successfully begin global transaction xid = {}", gt.XID)
	return &apis.GlobalBeginResponse{
		ResultCode: apis.ResultCodeSuccess,
		XID:        xid,
	}, nil
}

// GetStatus 获取全局事务状态
func (tc *TransactionCoordinator) GetStatus(ctx context.Context, request *apis.GlobalStatusRequest) (*apis.GlobalStatusResponse, error) {
	gs := tc.holder.FindGlobalSession(request.XID)
	if gs != nil {
		return &apis.GlobalStatusResponse{
			ResultCode:   apis.ResultCodeSuccess,
			GlobalStatus: gs.Status,
		}, nil
	}
	return &apis.GlobalStatusResponse{
		ResultCode:   apis.ResultCodeSuccess,
		GlobalStatus: apis.Finished,
	}, nil
}

func (tc *TransactionCoordinator) GlobalReport(ctx context.Context, request *apis.GlobalReportRequest) (*apis.GlobalReportResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GlobalReport not implemented")
}

// Commit 全局事务提交
func (tc *TransactionCoordinator) Commit(ctx context.Context, request *apis.GlobalCommitRequest) (*apis.GlobalCommitResponse, error) {
	// 获取全局事务以及对应的分支分支事务
	gt := tc.holder.FindGlobalTransaction(request.XID)
	if gt == nil {
		return &apis.GlobalCommitResponse{
			ResultCode:   apis.ResultCodeSuccess,
			GlobalStatus: apis.Finished,
		}, nil
	}

	// 删除行锁，更新全局事务状态
	shouldCommit, err := func(gt *model.GlobalTransaction) (bool, error) {
		result, err := tc.locker.TryLock(gt.GlobalSession, time.Duration(gt.Timeout)*time.Millisecond)
		if err != nil {
			return false, err
		}
		if result {
			defer tc.locker.Unlock(gt.GlobalSession)
			// 如果全局事务是活跃状态 设置成不活跃状态 避免再注册分支事务
			if gt.Active {
				// Active need persistence
				// Highlight: Firstly, close the session, then no more branch can be registered.
				// 首先，关闭会话，然后没有更多的分支可以注册。
				err = tc.holder.InactiveGlobalSession(gt.GlobalSession)
				if err != nil {
					return false, err
				}
			}
			// 删除所有行锁
			tc.resourceDataLocker.ReleaseGlobalSessionLock(gt)
			if gt.Status == apis.Begin {
				err = tc.holder.UpdateGlobalSessionStatus(gt.GlobalSession, apis.Committing)
				if err != nil {
					return false, err
				}
				return true, nil
			}
			return false, nil
		}
		return false, fmt.Errorf("failed to lock global transaction xid = %s", request.XID)
	}(gt)

	if err != nil {
		return &apis.GlobalCommitResponse{
			ResultCode:    apis.ResultCodeFailed,
			ExceptionCode: apis.FailedLockGlobalTransaction,
			Message:       err.Error(),
			GlobalStatus:  gt.Status,
		}, nil
	}

	// 非begin状态 重复commit
	if !shouldCommit {
		// 异步提交返回Committed状态
		if gt.Status == apis.AsyncCommitting {
			return &apis.GlobalCommitResponse{
				ResultCode:   apis.ResultCodeSuccess,
				GlobalStatus: apis.Committed,
			}, nil
		}
		// 同步提交返回全局事务当前状态
		return &apis.GlobalCommitResponse{
			ResultCode:   apis.ResultCodeSuccess,
			GlobalStatus: gt.Status,
		}, nil
	}

	// 异步提交
	if gt.CanBeCommittedAsync() {
		err = tc.holder.UpdateGlobalSessionStatus(gt.GlobalSession, apis.AsyncCommitting)
		if err != nil {
			return nil, err
		}

		// 提交成功
		return &apis.GlobalCommitResponse{
			ResultCode:   apis.ResultCodeSuccess,
			GlobalStatus: apis.Committed,
		}, nil
	}

	// 同步提交
	_, err = tc.doGlobalCommit(gt, false)
	if err != nil {
		return &apis.GlobalCommitResponse{
			ResultCode:    apis.ResultCodeFailed,
			ExceptionCode: apis.UnknownErr,
			Message:       err.Error(),
			GlobalStatus:  gt.Status,
		}, nil
	}
	return &apis.GlobalCommitResponse{
		ResultCode:   apis.ResultCodeSuccess,
		GlobalStatus: apis.Committed,
	}, nil
}

// 全局事务提交 retrying是否后台重试 后台发起的不再进行重试
func (tc *TransactionCoordinator) doGlobalCommit(gt *model.GlobalTransaction, retrying bool) (bool, error) {
	var err error
	runtime.GoWithRecover(func() {
		evt := event.NewGlobalTransactionEvent(gt.TransactionID, event.RoleTC, gt.TransactionName, gt.BeginTime, 0, gt.Status)
		event.EventBus.GlobalTransactionEventChannel <- evt
	}, nil)

	if gt.IsSaga() {
		return false, status.Errorf(codes.Unimplemented, "method Commit not supported saga mode")
	}

	for bs := range gt.BranchSessions {
		// 有分支事务失败还commit，只能跳过了
		if bs.Status == apis.PhaseOneFailed {
			tc.resourceDataLocker.ReleaseLock(bs) // 释放分支事务行锁
			delete(gt.BranchSessions, bs)
			err = tc.holder.RemoveBranchSession(gt.GlobalSession, bs)
			if err != nil {
				return false, err
			}
			continue
		}
		branchStatus, err1 := tc.branchCommit(bs) // 分支事务提交
		if err1 != nil {
			log.Errorf("exception committing branch xid=%d branchID=%d, err: %v", bs.GetXID(), bs.BranchID, err1)
			if !retrying { // 后续重试
				err = tc.holder.UpdateGlobalSessionStatus(gt.GlobalSession, apis.CommitRetrying)
				if err != nil {
					return false, err
				}
			}
			return false, err1
		}

		switch branchStatus {
		case apis.PhaseTwoCommitted: // 提交成功
			tc.resourceDataLocker.ReleaseLock(bs)                     // 删除分支事务行锁
			delete(gt.BranchSessions, bs)                             // 删除当前信息
			err = tc.holder.RemoveBranchSession(gt.GlobalSession, bs) // 删除分支事务
			if err != nil {
				return false, err
			}
			continue
		case apis.PhaseTwoCommitFailedCanNotRetry: // 提交失败，并且不能重试 删除全部事务信息
			{
				if gt.CanBeCommittedAsync() {
					log.Errorf("by [%s], failed to commit branch %v", bs.Status.String(), bs)
					continue
				} else {
					// change status first, if need retention global session data,
					// might not remove global session, then, the status is very important.
					err = tc.holder.UpdateGlobalSessionStatus(gt.GlobalSession, apis.CommitFailed)
					if err != nil {
						return false, err
					}
					tc.resourceDataLocker.ReleaseGlobalSessionLock(gt)
					err = tc.holder.RemoveGlobalTransaction(gt)
					if err != nil {
						return false, err
					}
					log.Errorf("finally, failed to commit global[%d] since branch[%d] commit failed", gt.XID, bs.BranchID)
					return false, nil
				}
			}
		default:
			{
				if !retrying {
					err = tc.holder.UpdateGlobalSessionStatus(gt.GlobalSession, apis.CommitRetrying)
					if err != nil {
						return false, err
					}
					return false, nil
				}
				if gt.CanBeCommittedAsync() {
					log.Errorf("by [%s], failed to commit branch %v", bs.Status.String(), bs)
					continue
				} else {
					log.Errorf("failed to commit global[%d] since branch[%d] commit failed, will retry later.", gt.XID, bs.BranchID)
					return false, nil
				}
			}
		}
	}

	gs := tc.holder.FindGlobalTransaction(gt.XID)
	if gs != nil && gs.HasBranch() {
		log.Infof("global[%d] committing is NOT done.", gt.XID)
		return false, nil
	}

	// change status first, if need retention global session data,
	// might not remove global session, then, the status is very important.
	// 首先改变状态，如果需要保留全局会话数据，可能不会删除全局会话，那么，状态是非常重要的。
	err = tc.holder.UpdateGlobalSessionStatus(gt.GlobalSession, apis.Committed)
	if err != nil {
		return false, err
	}

	tc.resourceDataLocker.ReleaseGlobalSessionLock(gt)
	err = tc.holder.RemoveGlobalTransaction(gt)
	if err != nil {
		return false, err
	}

	runtime.GoWithRecover(func() {
		evt := event.NewGlobalTransactionEvent(gt.TransactionID, event.RoleTC, gt.TransactionName, gt.BeginTime,
			int64(time2.CurrentTimeMillis()), gt.Status)
		event.EventBus.GlobalTransactionEventChannel <- evt
	}, nil)
	log.Infof("global[%d] committing is successfully done.", gt.XID)

	return true, err
}

// 分支事务提交
func (tc *TransactionCoordinator) branchCommit(bs *apis.BranchSession) (apis.BranchSession_BranchStatus, error) {
	request := &apis.BranchCommitRequest{
		XID:             bs.XID,
		BranchID:        bs.BranchID,
		ResourceID:      bs.ResourceID,
		LockKey:         bs.LockKey,
		BranchType:      bs.Type,
		ApplicationData: bs.ApplicationData,
	}

	content, err := types.MarshalAny(request)
	if err != nil {
		return bs.Status, err
	}

	message := &apis.BranchMessage{
		ID:                int64(tc.idGenerator.Inc()),
		BranchMessageType: apis.TypeBranchCommit,
		Message:           content,
	}

	// 异步阻塞等待
	queue, _ := tc.callBackMessages.LoadOrStore(bs.Addressing, NewCallbackMessageQueue())
	q := queue.(*CallbackMessageQueue)
	q.Enqueue(message)

	resp := common2.NewMessageFuture(message)
	tc.futures.Store(message.ID, resp)

	timer := time.NewTimer(tc.streamMessageTimeout)
	select {
	case <-timer.C:
		tc.futures.Delete(resp.ID)
		return bs.Status, fmt.Errorf("wait branch commit response timeout")
	case <-resp.Done:
		timer.Stop()
	}

	response, ok := resp.Response.(*apis.BranchCommitResponse)
	if !ok {
		log.Infof("rollback response: %v", resp.Response)
		return bs.Status, fmt.Errorf("response type not right")
	}
	if response.ResultCode == apis.ResultCodeSuccess {
		return response.BranchStatus, nil
	}
	return bs.Status, fmt.Errorf(response.Message)
}

// Rollback 全局事务回滚
func (tc *TransactionCoordinator) Rollback(ctx context.Context, request *apis.GlobalRollbackRequest) (*apis.GlobalRollbackResponse, error) {
	// 事务信息
	gt := tc.holder.FindGlobalTransaction(request.XID)
	if gt == nil {
		return &apis.GlobalRollbackResponse{
			ResultCode:   apis.ResultCodeSuccess,
			GlobalStatus: apis.Finished,
		}, nil
	}
	// 能否回滚
	shouldRollBack, err := func(gt *model.GlobalTransaction) (bool, error) {
		result, err := tc.locker.TryLock(gt.GlobalSession, time.Duration(gt.Timeout)*time.Millisecond)
		if err != nil {
			return false, err
		}
		if result {
			defer tc.locker.Unlock(gt.GlobalSession)
			if gt.Active {
				// Active need persistence
				// Highlight: Firstly, close the session, then no more branch can be registered.
				err = tc.holder.InactiveGlobalSession(gt.GlobalSession)
				if err != nil {
					return false, err
				}
			}
			if gt.Status == apis.Begin {
				err = tc.holder.UpdateGlobalSessionStatus(gt.GlobalSession, apis.RollingBack)
				if err != nil {
					return false, err
				}
				return true, nil
			}
			return false, nil
		}
		return false, fmt.Errorf("failed to lock global transaction xid = %s", request.XID)
	}(gt)

	if err != nil {
		return &apis.GlobalRollbackResponse{
			ResultCode:    apis.ResultCodeFailed,
			ExceptionCode: apis.FailedLockGlobalTransaction,
			Message:       err.Error(),
			GlobalStatus:  gt.Status,
		}, nil
	}

	// 不能回滚
	if !shouldRollBack {
		return &apis.GlobalRollbackResponse{
			ResultCode:   apis.ResultCodeSuccess,
			GlobalStatus: gt.Status,
		}, nil
	}

	// 回滚
	_, err = tc.doGlobalRollback(gt, false)
	if err != nil {
		return nil, err
	}

	return &apis.GlobalRollbackResponse{
		ResultCode:   apis.ResultCodeSuccess,
		GlobalStatus: gt.Status,
	}, nil
}

// 回滚全局事务 retrying是否重试
func (tc *TransactionCoordinator) doGlobalRollback(gt *model.GlobalTransaction, retrying bool) (bool, error) {
	var err error
	runtime.GoWithRecover(func() {
		evt := event.NewGlobalTransactionEvent(gt.TransactionID, event.RoleTC, gt.TransactionName, gt.BeginTime, 0, gt.Status)
		event.EventBus.GlobalTransactionEventChannel <- evt
	}, nil)

	if gt.IsSaga() {
		return false, status.Errorf(codes.Unimplemented, "method Commit not supported saga mode")
	}

	for bs := range gt.BranchSessions {
		// 分支事务已经失败，本地未提交
		if bs.Status == apis.PhaseOneFailed {
			tc.resourceDataLocker.ReleaseLock(bs)                     // 释放行锁
			delete(gt.BranchSessions, bs)                             // 删除当前数据
			err = tc.holder.RemoveBranchSession(gt.GlobalSession, bs) // 删除分支事务
			if err != nil {
				return false, err
			}
			continue
		}
		branchStatus, err1 := tc.branchRollback(bs)
		if err1 != nil { // 分支事务回滚失败 未响应
			log.Errorf("exception rolling back branch xid=%d branchID=%d, err: %v", gt.XID, bs.BranchID, err1)
			if !retrying {
				if gt.IsTimeoutGlobalStatus() { // 已经超时过
					err = tc.holder.UpdateGlobalSessionStatus(gt.GlobalSession, apis.TimeoutRollbackRetrying)
					if err != nil {
						return false, err
					}
				} else { // 未超时
					err = tc.holder.UpdateGlobalSessionStatus(gt.GlobalSession, apis.RollbackRetrying)
					if err != nil {
						return false, err
					}
				}
			}
			return false, err1
		}

		switch branchStatus {
		case apis.PhaseTwoRolledBack: // 回滚成功
			tc.resourceDataLocker.ReleaseLock(bs)                     // 释放行锁
			delete(gt.BranchSessions, bs)                             // 删除本地数据
			err = tc.holder.RemoveBranchSession(gt.GlobalSession, bs) // 删除分支事务
			if err != nil {
				return false, err
			}
			log.Infof("successfully rollback branch xid=%d branchID=%d", gt.XID, bs.BranchID)
			continue
		case apis.PhaseTwoRollbackFailedCanNotRetry:
			if gt.IsTimeoutGlobalStatus() {
				err = tc.holder.UpdateGlobalSessionStatus(gt.GlobalSession, apis.TimeoutRollbackFailed)
				if err != nil {
					return false, err
				}
			} else {
				err = tc.holder.UpdateGlobalSessionStatus(gt.GlobalSession, apis.RollbackFailed)
				if err != nil {
					return false, err
				}
			}
			tc.resourceDataLocker.ReleaseGlobalSessionLock(gt)
			err = tc.holder.RemoveGlobalTransaction(gt)
			if err != nil {
				return false, err
			}
			log.Infof("failed to rollback branch and stop retry xid=%d branchID=%d", gt.XID, bs.BranchID)
			return false, nil
		default:
			log.Infof("failed to rollback branch xid=%d branchID=%d", gt.XID, bs.BranchID)
			if !retrying {
				if gt.IsTimeoutGlobalStatus() {
					err = tc.holder.UpdateGlobalSessionStatus(gt.GlobalSession, apis.TimeoutRollbackRetrying)
					if err != nil {
						return false, err
					}
				} else {
					err = tc.holder.UpdateGlobalSessionStatus(gt.GlobalSession, apis.RollbackRetrying)
					if err != nil {
						return false, err
					}
				}
			}
			return false, nil
		}
	}

	// In db mode, there is a problem of inconsistent data in multiple copies, resulting in new branch
	// transaction registration when rolling back.
	// 1. New branch transaction and rollback branch transaction have no data association
	// 2. New branch transaction has data association with rollback branch transaction
	// The second query can solve the first problem, and if it is the second problem, it may cause a rollback
	// failure due to data changes.
	// 在db模式下，存在多个副本数据不一致的问题，导致回滚时注册新的分支事务。
	// 1.新分支事务和回滚分支事务没有数据关联
	// 2.新分支事务与回滚分支事务有数据关联
	// 第二个查询可以解决第一个问题，如果是第二个问题，可能会由于数据变化导致回滚失败。
	// 分支事务全部回滚成功之后校验是否还存在分支事务
	gs := tc.holder.FindGlobalTransaction(gt.XID)
	if gs != nil && gs.HasBranch() {
		log.Infof("Global[%d] rolling back is NOT done.", gt.XID)
		return false, nil
	}

	if gt.IsTimeoutGlobalStatus() {
		err = tc.holder.UpdateGlobalSessionStatus(gt.GlobalSession, apis.TimeoutRolledBack)
		if err != nil {
			return false, err
		}
	} else {
		err = tc.holder.UpdateGlobalSessionStatus(gt.GlobalSession, apis.RolledBack)
		if err != nil {
			return false, err
		}
	}
	tc.resourceDataLocker.ReleaseGlobalSessionLock(gt)
	err = tc.holder.RemoveGlobalTransaction(gt)
	if err != nil {
		return false, err
	}
	runtime.GoWithRecover(func() {
		evt := event.NewGlobalTransactionEvent(gt.TransactionID, event.RoleTC, gt.TransactionName, gt.BeginTime,
			int64(time2.CurrentTimeMillis()), gt.Status)
		event.EventBus.GlobalTransactionEventChannel <- evt
	}, nil)
	log.Infof("successfully rollback global, xid = %d", gt.XID)

	return true, err
}

// 分支事务回滚
func (tc *TransactionCoordinator) branchRollback(bs *apis.BranchSession) (apis.BranchSession_BranchStatus, error) {
	request := &apis.BranchRollbackRequest{
		XID:             bs.XID,
		BranchID:        bs.BranchID,
		ResourceID:      bs.ResourceID,
		LockKey:         bs.LockKey,
		BranchType:      bs.Type,
		ApplicationData: bs.ApplicationData,
	}

	content, err := types.MarshalAny(request)
	if err != nil {
		return bs.Status, err
	}
	message := &apis.BranchMessage{
		ID:                int64(tc.idGenerator.Inc()),
		BranchMessageType: apis.TypeBranchRollback,
		Message:           content,
	}

	// 添加客户端queue
	queue, _ := tc.callBackMessages.LoadOrStore(bs.Addressing, NewCallbackMessageQueue())
	q := queue.(*CallbackMessageQueue)
	q.Enqueue(message)

	// message响应
	resp := common2.NewMessageFuture(message)
	tc.futures.Store(message.ID, resp)

	// 阻塞等待message结果
	timer := time.NewTimer(tc.streamMessageTimeout)
	select {
	case <-timer.C:
		tc.futures.Delete(resp.ID)
		timer.Stop()
		return bs.Status, fmt.Errorf("wait branch rollback response timeout")
	case <-resp.Done:
		timer.Stop()
	}

	response := resp.Response.(*apis.BranchRollbackResponse)
	if response.ResultCode == apis.ResultCodeSuccess {
		return response.BranchStatus, nil
	}
	return bs.Status, fmt.Errorf(response.Message)
}

// BranchCommunicate Server-side的rpc
// 向客户端发送消息
func (tc *TransactionCoordinator) BranchCommunicate(stream apis.ResourceManagerService_BranchCommunicateServer) error {
	var addressing string
	done := make(chan bool)

	ctx := stream.Context()
	md, ok := metadata.FromIncomingContext(ctx)
	// 统计address的activeApplications用完-1
	if ok {
		addressing = md.Get("addressing")[0]
		c, ok := tc.activeApplications.Load(addressing)
		if ok {
			count := c.(int)
			tc.activeApplications.Store(addressing, count+1)
		} else {
			tc.activeApplications.Store(addressing, 1)
		}
		defer func() {
			c, _ := tc.activeApplications.Load(addressing)
			count := c.(int)
			tc.activeApplications.Store(addressing, count-1)
		}()
	}

	// 获取address的message队列
	queue, _ := tc.callBackMessages.LoadOrStore(addressing, NewCallbackMessageQueue())
	q := queue.(*CallbackMessageQueue)

	// 从队列中读取请求发送给分支事务客户端
	runtime.GoWithRecover(func() {
		for {
			select {
			case _, ok := <-done:
				if !ok {
					return
				}
			default:
				msg := q.Dequeue()
				if msg == nil {
					break
				}
				err := stream.Send(msg) // 发送
				if err != nil {
					return
				}
			}
		}
	}, nil)

	// 读取客户端响应
	for {
		select {
		case <-ctx.Done():
			close(done)
			return ctx.Err()
		default:
			branchMessage, err := stream.Recv()
			if err == io.EOF { // 客户端链接关闭
				close(done)
				return nil
			}
			if err != nil {
				close(done)
				return err
			}
			switch branchMessage.GetBranchMessageType() {
			case apis.TypeBranchCommitResult: // 分支事务提交
				response := &apis.BranchCommitResponse{}
				data := branchMessage.GetMessage().GetValue()
				err := response.Unmarshal(data)
				if err != nil {
					log.Error(err)
					continue
				}
				resp, loaded := tc.futures.Load(branchMessage.ID)
				if loaded {
					future := resp.(*common2.MessageFuture)
					future.Response = response
					future.Done <- true
					tc.futures.Delete(branchMessage.ID)
				}
			case apis.TypeBranchRollBackResult: // 分支事务回滚
				response := &apis.BranchRollbackResponse{}
				data := branchMessage.GetMessage().GetValue()
				err := response.Unmarshal(data)
				if err != nil {
					log.Error(err)
					continue
				}
				// 解除上游的阻塞
				resp, loaded := tc.futures.Load(branchMessage.ID)
				if loaded {
					future := resp.(*common2.MessageFuture)
					future.Response = response
					future.Done <- true
					tc.futures.Delete(branchMessage.ID)
				}
			}
		}
	}
}

// BranchRegister 注册分支事务
func (tc *TransactionCoordinator) BranchRegister(ctx context.Context, request *apis.BranchRegisterRequest) (*apis.BranchRegisterResponse, error) {
	// 全局事务 以及 分支事务
	gt := tc.holder.FindGlobalTransaction(request.XID)
	if gt == nil {
		log.Errorf("could not found global transaction xid = %s", request.XID)
		return &apis.BranchRegisterResponse{
			ResultCode:    apis.ResultCodeFailed,
			ExceptionCode: apis.GlobalTransactionNotExist,
			Message:       fmt.Sprintf("could not found global transaction xid = %s", request.XID),
		}, nil
	}

	result, err := tc.locker.TryLock(gt.GlobalSession, time.Duration(gt.Timeout)*time.Millisecond)
	if err != nil {
		return &apis.BranchRegisterResponse{
			ResultCode:    apis.ResultCodeFailed,
			ExceptionCode: apis.FailedLockGlobalTransaction,
			Message:       fmt.Sprintf("could not found global transaction xid = %s", request.XID),
		}, nil
	}
	if result {
		defer tc.locker.Unlock(gt.GlobalSession)
		// 非活跃状态
		if !gt.Active {
			return &apis.BranchRegisterResponse{
				ResultCode:    apis.ResultCodeFailed,
				ExceptionCode: apis.GlobalTransactionNotActive,
				Message:       fmt.Sprintf("could not register branch into global session xid = %s status = %d", gt.XID, gt.Status),
			}, nil
		}
		// 非begin状态
		if gt.Status != apis.Begin {
			return &apis.BranchRegisterResponse{
				ResultCode:    apis.ResultCodeFailed,
				ExceptionCode: apis.GlobalTransactionStatusInvalid,
				Message: fmt.Sprintf("could not register branch into global session xid = %s status = %d while expecting %d",
					gt.XID, gt.Status, apis.Begin),
			}, nil
		}

		// 分支事务
		bs := &apis.BranchSession{
			Addressing:      request.Addressing,
			XID:             request.XID,
			BranchID:        uuid.NextID(), // 分支事务id
			TransactionID:   gt.TransactionID,
			ResourceID:      request.ResourceID,
			LockKey:         request.LockKey,
			Type:            request.BranchType,
			Status:          apis.Registered, // 初始状态
			ApplicationData: request.ApplicationData,
		}

		if bs.Type == apis.AT {
			// AT模式加行锁
			result := tc.resourceDataLocker.AcquireLock(bs)
			if !result {
				return &apis.BranchRegisterResponse{
					ResultCode:    apis.ResultCodeFailed,
					ExceptionCode: apis.LockKeyConflict,
					Message: fmt.Sprintf("branch lock acquire failed xid = %s resourceId = %s, lockKey = %s",
						request.XID, request.ResourceID, request.LockKey),
				}, nil
			}
		}

		// 添加分支事务
		err := tc.holder.AddBranchSession(gt.GlobalSession, bs)
		if err != nil {
			log.Error(err)
			return &apis.BranchRegisterResponse{
				ResultCode:    apis.ResultCodeFailed,
				ExceptionCode: apis.BranchRegisterFailed,
				Message:       fmt.Sprintf("branch register failed, xid = %s, branchID = %d, err: %s", gt.XID, bs.BranchID, err.Error()),
			}, nil
		}

		return &apis.BranchRegisterResponse{
			ResultCode: apis.ResultCodeSuccess,
			BranchID:   bs.BranchID,
		}, nil
	}

	return &apis.BranchRegisterResponse{
		ResultCode:    apis.ResultCodeFailed,
		ExceptionCode: apis.FailedLockGlobalTransaction,
		Message:       fmt.Sprintf("failed to lock global transaction xid = %s", request.XID),
	}, nil
}

// BranchReport 分支事务状态上报
func (tc *TransactionCoordinator) BranchReport(ctx context.Context, request *apis.BranchReportRequest) (*apis.BranchReportResponse, error) {
	// 全局事务+分支事务
	gt := tc.holder.FindGlobalTransaction(request.XID)
	if gt == nil {
		log.Errorf("could not found global transaction xid = %s", request.XID)
		return &apis.BranchReportResponse{
			ResultCode:    apis.ResultCodeFailed,
			ExceptionCode: apis.GlobalTransactionNotExist,
			Message:       fmt.Sprintf("could not found global transaction xid = %s", request.XID),
		}, nil
	}

	// 获取指定的分支事务
	bs := gt.GetBranch(request.BranchID)
	if bs == nil {
		return &apis.BranchReportResponse{
			ResultCode:    apis.ResultCodeFailed,
			ExceptionCode: apis.BranchTransactionNotExist,
			Message:       fmt.Sprintf("could not found branch session xid = %s branchID = %d", gt.XID, request.BranchID),
		}, nil
	}

	// 更新分支事务状态
	err := tc.holder.UpdateBranchSessionStatus(bs, request.BranchStatus)
	if err != nil {
		return &apis.BranchReportResponse{
			ResultCode:    apis.ResultCodeFailed,
			ExceptionCode: apis.BranchReportFailed,
			Message:       fmt.Sprintf("branch report failed, xid = %s, branchID = %d, err: %s", gt.XID, bs.BranchID, err.Error()),
		}, nil
	}

	return &apis.BranchReportResponse{
		ResultCode: apis.ResultCodeSuccess,
	}, nil
}

// LockQuery 是否已存在行锁
func (tc *TransactionCoordinator) LockQuery(ctx context.Context, request *apis.GlobalLockQueryRequest) (*apis.GlobalLockQueryResponse, error) {
	result := tc.resourceDataLocker.IsLockable(request.XID, request.ResourceID, request.LockKey)
	return &apis.GlobalLockQueryResponse{
		ResultCode: apis.ResultCodeSuccess,
		Lockable:   result,
	}, nil
}

// 定时处理超时未提交的全局事务
func (tc *TransactionCoordinator) processTimeoutCheck() {
	for {
		timer := time.NewTimer(tc.timeoutRetryPeriod)

		<-timer.C
		tc.timeoutCheck()

		timer.Stop()
	}
}

func (tc *TransactionCoordinator) processRetryRollingBack() {
	for {
		timer := time.NewTimer(tc.rollingBackRetryPeriod)

		<-timer.C
		tc.handleRetryRollingBack()

		timer.Stop()
	}
}

func (tc *TransactionCoordinator) processRetryCommitting() {
	for {
		timer := time.NewTimer(tc.committingRetryPeriod)

		<-timer.C
		tc.handleRetryCommitting()

		timer.Stop()
	}
}

func (tc *TransactionCoordinator) processAsyncCommitting() {
	for {
		timer := time.NewTimer(tc.asyncCommittingRetryPeriod)

		<-timer.C
		tc.handleAsyncCommitting()

		timer.Stop()
	}
}

// 只是更新全局事务的状态
func (tc *TransactionCoordinator) timeoutCheck() {
	// 查找开始状态的全局事务
	sessions := tc.holder.FindGlobalSessions([]apis.GlobalSession_GlobalStatus{apis.Begin})
	if len(sessions) == 0 {
		return
	}
	for _, globalSession := range sessions {
		// 全局事务是否超时
		if isGlobalSessionTimeout(globalSession) {
			result, err := tc.locker.TryLock(globalSession, time.Duration(globalSession.Timeout)*time.Millisecond)
			if err == nil && result {
				if globalSession.Active {
					// Active need persistence
					// Highlight: Firstly, close the session, then no more branch can be registered.
					// 关闭会话，然后没有更多的分支事务可以注册。
					err = tc.holder.InactiveGlobalSession(globalSession)
					if err != nil {
						return
					}
				}
				// 更新全局事务状态

				err = tc.holder.UpdateGlobalSessionStatus(globalSession, apis.TimeoutRollingBack)
				if err != nil {
					return
				}

				tc.locker.Unlock(globalSession)
				// 事件
				evt := event.NewGlobalTransactionEvent(globalSession.TransactionID, event.RoleTC, globalSession.TransactionName, globalSession.BeginTime, 0, globalSession.Status)
				event.EventBus.GlobalTransactionEventChannel <- evt
			}
		}
	}
}

// 回滚重试
func (tc *TransactionCoordinator) handleRetryRollingBack() {
	addressingIdentities := tc.getAddressingIdentities()
	if len(addressingIdentities) == 0 {
		return
	}
	rollbackTransactions := tc.holder.FindRetryRollbackGlobalTransactions(addressingIdentities)
	if len(rollbackTransactions) == 0 {
		return
	}
	now := time2.CurrentTimeMillis()
	for _, transaction := range rollbackTransactions {
		if transaction.Status == apis.RollingBack && !transaction.IsRollingBackDead() {
			continue
		}
		if isRetryTimeout(int64(now), tc.maxRollbackRetryTimeout, transaction.BeginTime) {
			if tc.rollbackRetryTimeoutUnlockEnable {
				tc.resourceDataLocker.ReleaseGlobalSessionLock(transaction)
			}
			err := tc.holder.RemoveGlobalTransaction(transaction)
			if err != nil {
				log.Error(err)
			}
			log.Errorf("GlobalSession rollback retry timeout and removed [%s]", transaction.XID)
			continue
		}
		_, err := tc.doGlobalRollback(transaction, true)
		if err != nil {
			log.Errorf("failed to retry rollback [%s]", transaction.XID)
		}
	}
}

// 是否超过重试时间
func isRetryTimeout(now int64, timeout int64, beginTime int64) bool {
	if timeout >= AlwaysRetryBoundary && now-beginTime > timeout {
		return true
	}
	return false
}

// 重新提交
func (tc *TransactionCoordinator) handleRetryCommitting() {
	addressingIdentities := tc.getAddressingIdentities()
	if len(addressingIdentities) == 0 {
		return
	}
	// 需要CommitRetrying的全局事务
	committingTransactions := tc.holder.FindRetryCommittingGlobalTransactions(addressingIdentities)
	if len(committingTransactions) == 0 {
		return
	}
	now := time2.CurrentTimeMillis()
	for _, transaction := range committingTransactions {
		// 超过重试时间 直接删除事务
		if isRetryTimeout(int64(now), tc.maxCommitRetryTimeout, transaction.BeginTime) {
			err := tc.holder.RemoveGlobalTransaction(transaction)
			if err != nil {
				log.Error(err)
			}
			log.Errorf("GlobalSession commit retry timeout and removed [%s]", transaction.XID)
			continue
		}
		_, err := tc.doGlobalCommit(transaction, true)
		if err != nil {
			log.Errorf("failed to retry committing [%s]", transaction.XID)
		}
	}
}

// 异步提交的全局事务
func (tc *TransactionCoordinator) handleAsyncCommitting() {
	addressingIdentities := tc.getAddressingIdentities()
	if len(addressingIdentities) == 0 {
		return
	}
	asyncCommittingTransactions := tc.holder.FindAsyncCommittingGlobalTransactions(addressingIdentities)
	if len(asyncCommittingTransactions) == 0 {
		return
	}
	for _, transaction := range asyncCommittingTransactions {
		if transaction.Status != apis.AsyncCommitting {
			continue
		}
		// 提交全局事务
		_, err := tc.doGlobalCommit(transaction, true)
		if err != nil {
			log.Errorf("failed to async committing [%s]", transaction.XID)
		}
	}
}

// 获取当前建立链接的分支事务客户端adress
func (tc *TransactionCoordinator) getAddressingIdentities() []string {
	var addressIdentities []string
	tc.activeApplications.Range(func(key, value interface{}) bool {
		count := value.(int)
		if count > 0 {
			addressing := key.(string)
			addressIdentities = append(addressIdentities, addressing)
		}
		return true
	})
	return addressIdentities
}

// 全局事务是否超时
func isGlobalSessionTimeout(gt *apis.GlobalSession) bool {
	return (time2.CurrentTimeMillis() - uint64(gt.BeginTime)) > uint64(gt.Timeout)
}
