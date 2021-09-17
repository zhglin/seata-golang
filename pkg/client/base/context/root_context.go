package context

import (
	"context"
	"fmt"
	"strings"

	"github.com/opentrx/seata-golang/v2/pkg/apis"
	"github.com/opentrx/seata-golang/v2/pkg/util/log"
)

const (
	KeyXID                = "TX_XID" // 全局事务id
	KeyXIDInterceptorType = "tx-xid-interceptor-type"
	KeyGlobalLockFlag     = "TX_LOCK"
)

// RootContext store the global transaction context
type RootContext struct {
	context.Context

	// like thread local map
	localMap map[string]interface{}
}

// NewRootContext return a pointer to RootContext
// 创建rootContext，并绑定全局事务id
func NewRootContext(ctx context.Context) *RootContext {
	rootCtx := &RootContext{
		Context:  ctx,
		localMap: make(map[string]interface{}),
	}

	xID := ctx.Value(KeyXID)
	if xID != nil {
		xid := xID.(string)
		rootCtx.Bind(xid)
	}
	return rootCtx
}

// Set store key value to RootContext
// RootContext中存储键值
func (c *RootContext) Set(key string, value interface{}) {
	if c.localMap == nil {
		c.localMap = make(map[string]interface{})
	}
	c.localMap[key] = value
}

// Get get a value by given key from RootContext
func (c *RootContext) Get(key string) (value interface{}, exists bool) {
	value, exists = c.localMap[key]
	return
}

// GetXID from RootContext get xid
// 从RootContext中获取全局的事务id (xid)
func (c *RootContext) GetXID() string {
	// 存在直接返回
	xID := c.localMap[KeyXID]
	xid, ok := xID.(string)
	if ok && xid != "" {
		return xid
	}

	// todo
	xIDType := c.localMap[KeyXIDInterceptorType]
	xidType, success := xIDType.(string)

	if success && xidType != "" && strings.Contains(xidType, "_") {
		return strings.Split(xidType, "_")[0]
	}

	return ""
}

// GetXIDInterceptorType from RootContext get xid interceptor type
func (c *RootContext) GetXIDInterceptorType() string {
	xIDType := c.localMap[KeyXIDInterceptorType]
	xidType, _ := xIDType.(string)
	return xidType
}

// Bind bind xid with RootContext
// 绑定全局事务id(xid)
func (c *RootContext) Bind(xid string) {
	log.Debugf("bind %s", xid)
	c.Set(KeyXID, xid)
}

// BindInterceptorType bind interceptor type with RootContext
func (c *RootContext) BindInterceptorType(xidType string) {
	if xidType != "" {
		xidTypes := strings.Split(xidType, "_")

		if len(xidTypes) == 2 {
			c.BindInterceptorTypeWithBranchType(xidTypes[0],
				apis.BranchSession_BranchType(apis.BranchSession_BranchType_value[xidTypes[1]]))
		}
	}
}

// BindInterceptorTypeWithBranchType bind interceptor type and branch type with RootContext
func (c *RootContext) BindInterceptorTypeWithBranchType(xid string, branchType apis.BranchSession_BranchType) {
	xidType := fmt.Sprintf("%s_%s", xid, branchType.String())
	log.Debugf("bind interceptor type xid=%s branchType=%s", xid, branchType.String())
	c.Set(KeyXIDInterceptorType, xidType)
}

// BindGlobalLockFlag bind global lock flag with RootContext
func (c *RootContext) BindGlobalLockFlag() {
	log.Debug("local transaction global lock support enabled")
	c.Set(KeyGlobalLockFlag, KeyGlobalLockFlag)
}

// Unbind unbind xid with RootContext
// 从RootContext中删除全局事务xid
func (c *RootContext) Unbind() string {
	xID := c.localMap[KeyXID]
	xid, ok := xID.(string)
	if ok && xid != "" {
		log.Debugf("unbind %s", xid)
		delete(c.localMap, KeyXID)
		return xid
	}
	return ""

}

// UnbindInterceptorType unbind interceptor type with RootContext
func (c *RootContext) UnbindInterceptorType() string {
	xidType := c.localMap[KeyXIDInterceptorType]
	xt, ok := xidType.(string)
	if ok && xt != "" {
		log.Debugf("unbind inteceptor type %s", xidType)
		delete(c.localMap, KeyXIDInterceptorType)
		return xt
	}
	return ""
}

// UnbindGlobalLockFlag unbind global lock flag with RootContext
func (c *RootContext) UnbindGlobalLockFlag() {
	log.Debug("unbind global lock flag")
	delete(c.localMap, KeyGlobalLockFlag)
}

// InGlobalTransaction determine whether the context is in global transaction
// 确定上下文是否在全局事务中
func (c *RootContext) InGlobalTransaction() bool {
	return c.localMap[KeyXID] != nil
}

// RequireGlobalLock return global lock flag
func (c *RootContext) RequireGlobalLock() bool {
	_, exists := c.localMap[KeyGlobalLockFlag]
	return exists
}
