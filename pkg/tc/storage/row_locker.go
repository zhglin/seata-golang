package storage

import (
	"fmt"
	"strings"

	"github.com/opentrx/seata-golang/v2/pkg/apis"
	"github.com/opentrx/seata-golang/v2/pkg/util/common"
)

const LockSplit = "^^^"

// CollectBranchSessionRowLocks 获取所有分支事务的行锁
func CollectBranchSessionRowLocks(branchSession *apis.BranchSession) []*apis.RowLock {
	if branchSession == nil || branchSession.LockKey == "" {
		return nil
	}
	return collectRowLocks(branchSession.LockKey, branchSession.ResourceID, branchSession.XID, branchSession.TransactionID, branchSession.BranchID)
}

func CollectRowLocks(lockKey string, resourceID string, xid string) []*apis.RowLock {
	return collectRowLocks(lockKey, resourceID, xid, common.GetTransactionID(xid), 0)
}

// lockKey table:[]pks;table:[]pks
func collectRowLocks(lockKey string,
	resourceID string,
	xid string,
	transactionID int64,
	branchID int64) []*apis.RowLock {
	var locks = make([]*apis.RowLock, 0)
	// 拆分lockKey 一个分支事务可以多张表多个行
	tableGroupedLockKeys := strings.Split(lockKey, ";")
	for _, tableGroupedLockKey := range tableGroupedLockKeys {
		if tableGroupedLockKey != "" {
			idx := strings.Index(tableGroupedLockKey, ":")
			if idx < 0 {
				return nil
			}

			tableName := tableGroupedLockKey[0:idx]  // 表名
			mergedPKs := tableGroupedLockKey[idx+1:] // 主键

			if mergedPKs == "" {
				return nil
			}

			pks := strings.Split(mergedPKs, ",") // 切分主键
			if len(pks) == 0 {
				return nil
			}

			for _, pk := range pks {
				if pk != "" {
					rowLock := &apis.RowLock{
						XID:           xid,
						TransactionID: transactionID,
						BranchID:      branchID,
						ResourceID:    resourceID,
						TableName:     tableName,
						PK:            pk,
						RowKey:        getRowKey(resourceID, tableName, pk), // 行锁
					}
					locks = append(locks, rowLock)
				}
			}
		}
	}
	return locks
}

// 行锁标识
func getRowKey(resourceID string, tableName string, pk string) string {
	return fmt.Sprintf("%s^^^%s^^^%s", resourceID, tableName, pk)
}
