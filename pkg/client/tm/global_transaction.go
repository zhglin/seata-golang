package tm

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/opentrx/seata-golang/v2/pkg/apis"
	ctx "github.com/opentrx/seata-golang/v2/pkg/client/base/context"
	"github.com/opentrx/seata-golang/v2/pkg/client/config"
	"github.com/opentrx/seata-golang/v2/pkg/util/log"
)

const (
	DEFAULT_GLOBAL_TX_TIMEOUT = 60000
	DEFAULT_GLOBAL_TX_NAME    = "default"
)

type SuspendedResourcesHolder struct {
	Xid string
}

type GlobalTransaction interface {
	Begin(ctx *ctx.RootContext) error
	BeginWithTimeout(timeout int32, ctx *ctx.RootContext) error
	BeginWithTimeoutAndName(timeout int32, name string, ctx *ctx.RootContext) error
	Commit(ctx *ctx.RootContext) error
	Rollback(ctx *ctx.RootContext) error
	Suspend(unbindXid bool, ctx *ctx.RootContext) (*SuspendedResourcesHolder, error)
	Resume(suspendedResourcesHolder *SuspendedResourcesHolder, ctx *ctx.RootContext) error
	GetStatus(ctx *ctx.RootContext) (apis.GlobalSession_GlobalStatus, error)
	GetXid(ctx *ctx.RootContext) string
	GlobalReport(globalStatus apis.GlobalSession_GlobalStatus, ctx *ctx.RootContext) error
	GetLocalStatus() apis.GlobalSession_GlobalStatus
}

type GlobalTransactionRole byte

const (
	// Launcher The Launcher. The one begins the current global transaction.
	// 开始当前全局事务。
	Launcher GlobalTransactionRole = iota

	// Participant The Participant. The one just joins into a existing global transaction.
	// 参与到现有的全局事务。
	Participant
)

func (role GlobalTransactionRole) String() string {
	switch role {
	case Launcher:
		return "Launcher"
	case Participant:
		return "Participant"
	default:
		return fmt.Sprintf("%d", role)
	}
}

type DefaultGlobalTransaction struct {
	conf               config.TMConfig
	XID                string
	Status             apis.GlobalSession_GlobalStatus
	Role               GlobalTransactionRole
	transactionManager TransactionManagerInterface
}

func (gtx *DefaultGlobalTransaction) Begin(ctx *ctx.RootContext) error {
	return gtx.BeginWithTimeout(DEFAULT_GLOBAL_TX_TIMEOUT, ctx)
}

func (gtx *DefaultGlobalTransaction) BeginWithTimeout(timeout int32, ctx *ctx.RootContext) error {
	return gtx.BeginWithTimeoutAndName(timeout, DEFAULT_GLOBAL_TX_NAME, ctx)
}

// BeginWithTimeoutAndName 开启全局事务
func (gtx *DefaultGlobalTransaction) BeginWithTimeoutAndName(timeout int32, name string, ctx *ctx.RootContext) error {
	// 非开启全局事务并且xid为空
	if gtx.Role != Launcher {
		if gtx.XID == "" {
			return errors.New("xid should not be empty")
		}
		log.Debugf("Ignore Begin(): just involved in global transaction [%s]", gtx.XID)
		return nil
	}

	// 全局事务id必须为空
	if gtx.XID != "" {
		return errors.New("xid should be empty")
	}

	// context中存在全局事务id
	if ctx.InGlobalTransaction() {
		return errors.New("xid should be empty")
	}
	xid, err := gtx.transactionManager.Begin(ctx, name, timeout)
	if err != nil {
		return errors.WithStack(err)
	}

	// 设置全局事务id，状态
	gtx.XID = xid
	gtx.Status = apis.Begin
	ctx.Bind(xid)
	log.Infof("begin new global transaction [%s]", xid)
	return nil
}

func (gtx *DefaultGlobalTransaction) Commit(ctx *ctx.RootContext) error {
	defer func() {
		ctxXid := ctx.GetXID()
		if ctxXid != "" && gtx.XID == ctxXid {
			ctx.Unbind()
		}
	}()
	if gtx.Role == Participant {
		log.Debugf("ignore Commit(): just involved in global transaction [%s]", gtx.XID)
		return nil
	}
	if gtx.XID == "" {
		return errors.New("xid should not be empty")
	}
	retry := gtx.conf.CommitRetryCount
	for retry > 0 {
		status, err := gtx.transactionManager.Commit(ctx, gtx.XID)
		if err != nil {
			log.Errorf("failed to report global commit [%s],Retry Countdown: %d, reason: %s", gtx.XID, retry, err.Error())
		} else {
			gtx.Status = status
			break
		}
		retry--
		if retry == 0 {
			return errors.New("Failed to report global commit")
		}
	}
	log.Infof("[%s] commit status: %s", gtx.XID, gtx.Status.String())
	return nil
}

func (gtx *DefaultGlobalTransaction) Rollback(ctx *ctx.RootContext) error {
	defer func() {
		ctxXid := ctx.GetXID()
		if ctxXid != "" && gtx.XID == ctxXid {
			ctx.Unbind()
		}
	}()
	if gtx.Role == Participant {
		log.Debugf("ignore Rollback(): just involved in global transaction [%s]", gtx.XID)
		return nil
	}
	if gtx.XID == "" {
		return errors.New("xid should not be empty")
	}
	retry := gtx.conf.RollbackRetryCount
	for retry > 0 {
		status, err := gtx.transactionManager.Rollback(ctx, gtx.XID)
		if err != nil {
			log.Errorf("failed to report global rollback [%s],Retry Countdown: %d, reason: %s", gtx.XID, retry, err.Error())
		} else {
			gtx.Status = status
			break
		}
		retry--
		if retry == 0 {
			return errors.New("Failed to report global rollback")
		}
	}
	log.Infof("[%s] rollback status: %s", gtx.XID, gtx.Status.String())
	return nil
}

// Suspend 解绑全局事务id
func (gtx *DefaultGlobalTransaction) Suspend(unbindXid bool, ctx *ctx.RootContext) (*SuspendedResourcesHolder, error) {
	xid := ctx.GetXID()
	if xid != "" && unbindXid {
		ctx.Unbind() // 删除
		log.Debugf("suspending current transaction,xid = %s", xid)
	}
	return &SuspendedResourcesHolder{Xid: xid}, nil
}

func (gtx *DefaultGlobalTransaction) Resume(suspendedResourcesHolder *SuspendedResourcesHolder, ctx *ctx.RootContext) error {
	if suspendedResourcesHolder == nil {
		return nil
	}
	xid := suspendedResourcesHolder.Xid
	if xid != "" {
		ctx.Bind(xid)
		log.Debugf("resuming the transaction,xid = %s", xid)
	}
	return nil
}

func (gtx *DefaultGlobalTransaction) GetStatus(ctx *ctx.RootContext) (apis.GlobalSession_GlobalStatus, error) {
	if gtx.XID == "" {
		return apis.UnknownGlobalStatus, nil
	}
	status, err := gtx.transactionManager.GetStatus(ctx, gtx.XID)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	gtx.Status = status
	return gtx.Status, nil
}

func (gtx *DefaultGlobalTransaction) GetXid(ctx *ctx.RootContext) string {
	return gtx.XID
}

func (gtx *DefaultGlobalTransaction) GlobalReport(globalStatus apis.GlobalSession_GlobalStatus, ctx *ctx.RootContext) error {
	defer func() {
		ctxXid := ctx.GetXID()
		if ctxXid != "" && gtx.XID == ctxXid {
			ctx.Unbind()
		}
	}()

	if gtx.XID == "" {
		return errors.New("xid should not be empty")
	}

	if globalStatus == 0 {
		return errors.New("globalStatus should not be zero")
	}

	status, err := gtx.transactionManager.GlobalReport(ctx, gtx.XID, globalStatus)
	if err != nil {
		return errors.WithStack(err)
	}

	gtx.Status = status
	log.Infof("[%s] report status: %s", gtx.XID, gtx.Status.String())
	return nil
}

func (gtx *DefaultGlobalTransaction) GetLocalStatus() apis.GlobalSession_GlobalStatus {
	return gtx.Status
}

// CreateNew 创建默认的
func CreateNew() *DefaultGlobalTransaction {
	return &DefaultGlobalTransaction{
		conf:               config.GetTMConfig(),
		XID:                "",
		Status:             apis.UnknownGlobalStatus,
		Role:               Launcher,
		transactionManager: defaultTransactionManager,
	}
}

// GetCurrent 根据ctx信息创建有事务id的
func GetCurrent(ctx *ctx.RootContext) *DefaultGlobalTransaction {
	xid := ctx.GetXID()
	if xid == "" {
		return nil
	}
	return &DefaultGlobalTransaction{
		conf:               config.GetTMConfig(),
		XID:                xid,
		Status:             apis.Begin,
		Role:               Participant,
		transactionManager: defaultTransactionManager,
	}
}

// GetCurrentOrCreate 全局事务管理器
// 如果没有全局事务id就新建
func GetCurrentOrCreate(ctx *ctx.RootContext) *DefaultGlobalTransaction {
	tx := GetCurrent(ctx)
	if tx == nil {
		return CreateNew()
	}
	return tx
}
