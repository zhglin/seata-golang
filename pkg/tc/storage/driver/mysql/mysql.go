package mysql

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"xorm.io/builder"

	"github.com/opentrx/seata-golang/v2/pkg/apis"
	"github.com/opentrx/seata-golang/v2/pkg/tc/storage"
	"github.com/opentrx/seata-golang/v2/pkg/tc/storage/driver/factory"
	"github.com/opentrx/seata-golang/v2/pkg/util/log"
	"github.com/opentrx/seata-golang/v2/pkg/util/sql"
)

const (
	InsertGlobalTransaction = `insert into %s (addressing, xid, transaction_id, transaction_name, timeout, begin_time, 
		status, active, gmt_create, gmt_modified) values(?, ?, ?, ?, ?, ?, ?, ?, now(), now())`

	QueryGlobalTransactionByXid = `select addressing, xid, transaction_id, transaction_name, timeout, begin_time, 
		status, active, gmt_create, gmt_modified from %s where xid = ?`

	UpdateGlobalTransaction = "update %s set status = ?, gmt_modified = now() where xid = ?"

	InactiveGlobalTransaction = "update %s set active = 0, gmt_modified = now() where xid = ?"

	DeleteGlobalTransaction = "delete from %s where xid = ?"

	InsertBranchTransaction = `insert into %s (addressing, xid, branch_id, transaction_id, resource_id, lock_key, branch_type,
        status, application_data, gmt_create, gmt_modified) values(?, ?, ?, ?, ?, ?, ?, ?, ?, now(), now())`

	QueryBranchTransaction = `select addressing, xid, branch_id, transaction_id, resource_id, lock_key, branch_type, status,
	    application_data, gmt_create, gmt_modified from %s where %s order by gmt_create asc`

	QueryBranchTransactionByXid = `select addressing, xid, branch_id, transaction_id, resource_id, lock_key, branch_type, status,
	    application_data, gmt_create, gmt_modified from %s where xid = ? order by gmt_create asc`

	UpdateBranchTransaction = "update %s set status = ?, gmt_modified = now() where xid = ? and branch_id = ?"

	DeleteBranchTransaction = "delete from %s where xid = ? and branch_id = ?"

	InsertRowLock = `insert into %s (xid, transaction_id, branch_id, resource_id, table_name, pk, row_key, gmt_create, 
		gmt_modified) values %s`

	QueryRowKey = `select xid, transaction_id, branch_id, resource_id, table_name, pk, row_key, gmt_create, gmt_modified
		from %s where %s order by gmt_create asc`
)

func init() {
	factory.Register("mysql", &mysqlFactory{})
}

type DriverParameters struct {
	DSN                string
	GlobalTable        string
	BranchTable        string
	LockTable          string
	QueryLimit         int
	MaxOpenConnections int
	MaxIdleConnections int
	MaxLifeTime        time.Duration
}

// mysqlFactory implements the factory.StorageDriverFactory interface
type mysqlFactory struct{}

func (factory *mysqlFactory) Create(parameters map[string]interface{}) (storage.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	engine      *xorm.Engine
	globalTable string
	branchTable string
	lockTable   string
	queryLimit  int
}

func FromParameters(parameters map[string]interface{}) (storage.StorageDriver, error) {
	dsn := parameters["dsn"]
	if dsn == nil {
		dsn = ""
	}

	globalTable := parameters["globaltable"]
	if globalTable == nil {
		dsn = "global_table"
	}

	branchTable := parameters["branchtable"]
	if globalTable == nil {
		dsn = "branch_table"
	}

	lockTable := parameters["locktable"]
	if globalTable == nil {
		dsn = "global_table"
	}

	queryLimit := 100
	ql := parameters["querylimit"]
	switch ql := ql.(type) {
	case string:
		var err error
		queryLimit, err = strconv.Atoi(ql)
		if err != nil {
			log.Error("the querylimit parameter should be a integer")
		}
	case int:
		queryLimit = ql
	case nil:
		// do nothing
	default:
		log.Error("the querylimit parameter should be a integer")
	}

	maxOpenConnections := 100
	mc := parameters["maxopenconnections"]
	switch mc := mc.(type) {
	case string:
		var err error
		maxOpenConnections, err = strconv.Atoi(mc)
		if err != nil {
			log.Error("the maxopenconnections parameter should be a integer")
		}
	case int:
		maxOpenConnections = mc
	case nil:
		// do nothing
	default:
		log.Error("the maxopenconnections parameter should be a integer")
	}

	maxIdleConnections := 20
	mi := parameters["maxidleconnections"]
	switch mi := mi.(type) {
	case string:
		var err error
		maxOpenConnections, err = strconv.Atoi(mi)
		if err != nil {
			log.Error("the maxidleconnections parameter should be a integer")
		}
	case int:
		maxIdleConnections = mi
	case nil:
		// do nothing
	default:
		log.Error("the maxidleconnections parameter should be a integer")
	}

	maxlifetime := 4 * time.Hour
	ml := parameters["maxlifetime"]
	switch ml := ml.(type) {
	case string:
		var err error
		maxlifetime, err = time.ParseDuration(ml)
		if err != nil {
			log.Error("the maxlifetime parameter should be a duration")
		}
	case time.Duration:
		maxlifetime = ml
	case nil:
		// do nothing
	default:
		log.Error("the maxlifetime parameter should be a duration")
	}

	driverParameters := DriverParameters{
		DSN:                fmt.Sprint(dsn),
		GlobalTable:        fmt.Sprint(globalTable),
		BranchTable:        fmt.Sprint(branchTable),
		LockTable:          fmt.Sprint(lockTable),
		QueryLimit:         queryLimit,
		MaxOpenConnections: maxOpenConnections,
		MaxIdleConnections: maxIdleConnections,
		MaxLifeTime:        maxlifetime,
	}

	return New(driverParameters)
}

// New constructs a new Driver
func New(params DriverParameters) (storage.StorageDriver, error) {
	if params.DSN == "" {
		return nil, fmt.Errorf("the dsn parameter should not be empty")
	}
	engine, err := xorm.NewEngine("mysql", params.DSN)
	if err != nil {
		return nil, err
	}
	engine.SetMaxOpenConns(params.MaxOpenConnections)
	engine.SetMaxIdleConns(params.MaxIdleConnections)
	engine.SetConnMaxLifetime(params.MaxLifeTime)

	return &driver{
		engine:      engine,
		globalTable: params.GlobalTable,
		branchTable: params.BranchTable,
		lockTable:   params.LockTable,
		queryLimit:  params.QueryLimit,
	}, nil
}

// Add global session.
func (driver *driver) AddGlobalSession(session *apis.GlobalSession) error {
	_, err := driver.engine.Exec(fmt.Sprintf(InsertGlobalTransaction, driver.globalTable),
		session.Addressing, session.XID, session.TransactionID, session.TransactionName,
		session.Timeout, session.BeginTime, session.Status, session.Active)
	return err
}

// Find global session.
func (driver *driver) FindGlobalSession(xid string) *apis.GlobalSession {
	var globalTransaction apis.GlobalSession
	result, err := driver.engine.SQL(fmt.Sprintf(QueryGlobalTransactionByXid, driver.globalTable), xid).
		Get(&globalTransaction)
	if result {
		return &globalTransaction
	}
	if err != nil {
		log.Errorf(err.Error())
	}
	return nil
}

// Find global sessions list.
func (driver *driver) FindGlobalSessions(statuses []apis.GlobalSession_GlobalStatus) []*apis.GlobalSession {
	var globalSessions []*apis.GlobalSession
	err := driver.engine.Table(driver.globalTable).
		Where(builder.In("status", statuses)).
		OrderBy("gmt_modified").
		Limit(driver.queryLimit).
		Find(&globalSessions)

	if err != nil {
		log.Errorf(err.Error())
	}
	return globalSessions
}

// All sessions collection.
func (driver *driver) AllSessions() []*apis.GlobalSession {
	var globalSessions []*apis.GlobalSession
	err := driver.engine.Table(driver.globalTable).
		OrderBy("gmt_modified").
		Find(&globalSessions)

	if err != nil {
		log.Errorf(err.Error())
	}
	return globalSessions
}

// Update global session status.
func (driver *driver) UpdateGlobalSessionStatus(session *apis.GlobalSession, status apis.GlobalSession_GlobalStatus) error {
	_, err := driver.engine.Exec(fmt.Sprintf(UpdateGlobalTransaction, driver.globalTable), status, session.XID)
	return err
}

// Inactive global session.
func (driver *driver) InactiveGlobalSession(session *apis.GlobalSession) error {
	_, err := driver.engine.Exec(fmt.Sprintf(InactiveGlobalTransaction, driver.globalTable), session.XID)
	return err
}

// Remove global session.
func (driver *driver) RemoveGlobalSession(session *apis.GlobalSession) error {
	_, err := driver.engine.Exec(fmt.Sprintf(DeleteGlobalTransaction, driver.globalTable), session.XID)
	return err
}

// Add branch session.
func (driver *driver) AddBranchSession(globalSession *apis.GlobalSession, session *apis.BranchSession) error {
	_, err := driver.engine.Exec(fmt.Sprintf(InsertBranchTransaction, driver.branchTable),
		session.Addressing, session.XID, session.BranchID, session.TransactionID, session.ResourceID, session.LockKey,
		session.Type, session.Status, session.ApplicationData)
	return err
}

// Find branch session.
func (driver *driver) FindBranchSessions(xid string) []*apis.BranchSession {
	var branchTransactions []*apis.BranchSession
	err := driver.engine.SQL(fmt.Sprintf(QueryBranchTransactionByXid, driver.branchTable), xid).Find(&branchTransactions)
	if err != nil {
		log.Errorf(err.Error())
	}
	return branchTransactions
}

// Find branch session.
func (driver *driver) FindBatchBranchSessions(xids []string) []*apis.BranchSession {
	var (
		branchTransactions []*apis.BranchSession
		xidArgs            []interface{}
	)
	whereCond := fmt.Sprintf("xid in %s", sql.MysqlAppendInParam(len(xids)))
	for _, xid := range xids {
		xidArgs = append(xidArgs, xid)
	}
	err := driver.engine.SQL(fmt.Sprintf(QueryBranchTransaction, driver.branchTable, whereCond), xidArgs...).Find(&branchTransactions)

	if err != nil {
		log.Errorf(err.Error())
	}
	return branchTransactions
}

// Update branch session status.
func (driver *driver) UpdateBranchSessionStatus(session *apis.BranchSession, status apis.BranchSession_BranchStatus) error {
	_, err := driver.engine.Exec(fmt.Sprintf(UpdateBranchTransaction, driver.branchTable),
		status,
		session.XID,
		session.BranchID)
	return err
}

// Remove branch session.
func (driver *driver) RemoveBranchSession(globalSession *apis.GlobalSession, session *apis.BranchSession) error {
	_, err := driver.engine.Exec(fmt.Sprintf(DeleteBranchTransaction, driver.branchTable),
		session.XID,
		session.BranchID)
	return err
}

// AcquireLock Acquire lock boolean.
func (driver *driver) AcquireLock(rowLocks []*apis.RowLock) bool {
	locks, rowKeys := distinctByKey(rowLocks)
	var (
		existedRowLocks []*apis.RowLock
		rowKeyArgs      []interface{}
	)
	for _, rowKey := range rowKeys {
		rowKeyArgs = append(rowKeyArgs, rowKey)
	}
	whereCond := fmt.Sprintf("row_key in %s", sql.MysqlAppendInParam(len(rowKeys)))
	err := driver.engine.SQL(fmt.Sprintf(QueryRowKey, driver.lockTable, whereCond), rowKeyArgs...).Find(&existedRowLocks)
	if err != nil {
		log.Errorf(err.Error())
	}

	currentXID := locks[0].XID
	canLock := true
	existedRowKeys := make([]string, 0)
	unrepeatedLocks := make([]*apis.RowLock, 0)
	for _, rowLock := range existedRowLocks {
		if rowLock.XID != currentXID {
			log.Infof("row lock [%s] on %s:%s is holding by xid {%s} branchID {%d}", rowLock.RowKey, driver.lockTable, rowLock.TableName,
				rowLock.PK, rowLock.XID, rowLock.BranchID)
			canLock = false
			break
		}
		existedRowKeys = append(existedRowKeys, rowLock.RowKey)
	}
	if !canLock {
		return false
	}
	if len(existedRowKeys) > 0 {
		for _, lock := range locks {
			if !contains(existedRowKeys, lock.RowKey) {
				unrepeatedLocks = append(unrepeatedLocks, lock)
			}
		}
	} else {
		unrepeatedLocks = locks
	}

	if len(unrepeatedLocks) == 0 {
		return true
	}

	var (
		sb        strings.Builder
		args      []interface{}
		sqlOrArgs []interface{}
	)
	for i := 0; i < len(unrepeatedLocks); i++ {
		sb.WriteString("(?, ?, ?, ?, ?, ?, ?, now(), now()),")
		args = append(args, unrepeatedLocks[i].XID, unrepeatedLocks[i].TransactionID, unrepeatedLocks[i].BranchID,
			unrepeatedLocks[i].ResourceID, unrepeatedLocks[i].TableName, unrepeatedLocks[i].PK, unrepeatedLocks[i].RowKey)
	}
	values := sb.String()
	valueStr := values[:len(values)-1]

	sqlOrArgs = append(sqlOrArgs, fmt.Sprintf(InsertRowLock, driver.lockTable, valueStr))
	sqlOrArgs = append(sqlOrArgs, args...)
	_, err = driver.engine.Exec(sqlOrArgs...)
	if err != nil {
		// In an extremely high concurrency scenario, the row lock has been written to the database,
		// but the mysql driver reports invalid connection exception, and then re-registers the branch,
		// it will report the duplicate key exception.
		log.Errorf("row locks batch acquire failed, %v, %v", unrepeatedLocks, err)
		return false
	}
	return true
}

func distinctByKey(locks []*apis.RowLock) ([]*apis.RowLock, []string) {
	result := make([]*apis.RowLock, 0)
	rowKeys := make([]string, 0)
	lockMap := make(map[string]byte)
	for _, lockDO := range locks {
		l := len(lockMap)
		lockMap[lockDO.RowKey] = 0
		if len(lockMap) != l {
			result = append(result, lockDO)
			rowKeys = append(rowKeys, lockDO.RowKey)
		}
	}
	return result, rowKeys
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// ReleaseLock Unlock boolean.
func (driver *driver) ReleaseLock(rowLocks []*apis.RowLock) bool {
	if rowLocks != nil && len(rowLocks) == 0 {
		return true
	}
	rowKeys := make([]string, 0)
	for _, lock := range rowLocks {
		rowKeys = append(rowKeys, lock.RowKey)
	}

	var lock = apis.RowLock{}
	_, err := driver.engine.Table(driver.lockTable).
		Where(builder.In("row_key", rowKeys).And(builder.Eq{"xid": rowLocks[0].XID})).
		Delete(&lock)

	if err != nil {
		log.Errorf(err.Error())
		return false
	}
	return true
}

// IsLockable Is lockable boolean.
func (driver *driver) IsLockable(xid string, resourceID string, lockKey string) bool {
	locks := storage.CollectRowLocks(lockKey, resourceID, xid)
	var existedRowLocks []*apis.RowLock
	rowKeys := make([]interface{}, 0)
	for _, lockDO := range locks {
		rowKeys = append(rowKeys, lockDO.RowKey)
	}
	whereCond := fmt.Sprintf("row_key in %s", sql.MysqlAppendInParam(len(rowKeys)))

	err := driver.engine.SQL(fmt.Sprintf(QueryRowKey, driver.lockTable, whereCond), rowKeys...).Find(&existedRowLocks)
	if err != nil {
		log.Errorf(err.Error())
	}
	currentXID := locks[0].XID
	for _, rowLock := range existedRowLocks {
		if rowLock.XID != currentXID {
			return false
		}
	}
	return true
}
