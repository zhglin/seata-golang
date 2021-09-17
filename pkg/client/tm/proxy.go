package tm

import (
	"context"
	"reflect"

	"github.com/pkg/errors"

	ctx "github.com/opentrx/seata-golang/v2/pkg/client/base/context"
	"github.com/opentrx/seata-golang/v2/pkg/client/base/model"
	"github.com/opentrx/seata-golang/v2/pkg/client/proxy"
	"github.com/opentrx/seata-golang/v2/pkg/util/log"
)

// GlobalTransactionProxyService 服务代理接口
type GlobalTransactionProxyService interface {
	// GetProxyService 需要被代理的对象
	GetProxyService() interface{}
	// GetMethodTransactionInfo 每个全局函数的事务参数
	GetMethodTransactionInfo(methodName string) *model.TransactionInfo
}

var (
	typError = reflect.Zero(reflect.TypeOf((*error)(nil)).Elem()).Type()
)

// Implement 客户端设置全局事务的代理
// https://github.com/opentrx/seata-go-samples/blob/v2/http/aggregation_svc/svc/svc.go
func Implement(v GlobalTransactionProxyService) {
	valueOf := reflect.ValueOf(v)
	log.Debugf("[implement] reflect.TypeOf: %s", valueOf.String())

	valueOfElem := valueOf.Elem()
	typeOf := valueOfElem.Type()

	// check incoming interface, incoming interface's elem must be a struct.
	// 检查传入接口，传入接口的elem必须是一个结构体。
	if typeOf.Kind() != reflect.Struct {
		log.Errorf("%s must be a struct ptr", valueOf.String())
		return
	}
	proxyService := v.GetProxyService() // 被代理的对象

	makeCallProxy := func(methodDesc *proxy.MethodDescriptor, txInfo *model.TransactionInfo) func(in []reflect.Value) []reflect.Value {
		return func(in []reflect.Value) []reflect.Value {
			var (
				args                     []interface{}
				returnValues             []reflect.Value
				suspendedResourcesHolder *SuspendedResourcesHolder
			)

			if txInfo == nil {
				// testing phase, this problem should be resolved
				panic(errors.New("transactionInfo does not exist"))
			}

			inNum := len(in)
			if inNum+1 != methodDesc.ArgsNum {
				// testing phase, this problem should be resolved
				panic(errors.New("args does not match"))
			}

			// 从参数中查找context.Context，从中提前xid
			invCtx := ctx.NewRootContext(context.Background())
			for i := 0; i < inNum; i++ {
				if in[i].Type().String() == "context.Context" {
					if !in[i].IsNil() {
						// the user declared context as method's parameter
						invCtx = ctx.NewRootContext(in[i].Interface().(context.Context))
					}
				}
				args = append(args, in[i].Interface())
			}

			// 创建全局事务管理器
			tx := GetCurrentOrCreate(invCtx)
			defer func() {
				err := tx.Resume(suspendedResourcesHolder, invCtx)
				if err != nil {
					log.Error(err)
				}
			}()

			switch txInfo.Propagation {
			case model.Required:
			case model.RequiresNew: // 开启新事务，解绑原来的全局事务id
				suspendedResourcesHolder, _ = tx.Suspend(true, invCtx)
			case model.NotSupported: // 解绑全局事务，不支持
				suspendedResourcesHolder, _ = tx.Suspend(true, invCtx)
				returnValues = proxy.Invoke(methodDesc, invCtx, args)
				return returnValues
			case model.Supports: // 已存在全局事务，不再开启全局事务
				if !invCtx.InGlobalTransaction() {
					returnValues = proxy.Invoke(methodDesc, invCtx, args)
					return returnValues
				}
			case model.Never: // 存在报错，不存在不支持
				if invCtx.InGlobalTransaction() {
					return proxy.ReturnWithError(methodDesc, errors.Errorf("Existing transaction found for transaction marked with propagation 'never',xid = %s", invCtx.GetXID()))
				}
				returnValues = proxy.Invoke(methodDesc, invCtx, args)
				return returnValues
			case model.Mandatory: // 存在全局事务报错
				if !invCtx.InGlobalTransaction() {
					return proxy.ReturnWithError(methodDesc, errors.New("No existing transaction found for transaction marked with propagation 'mandatory'"))
				}
			default:
				return proxy.ReturnWithError(methodDesc, errors.Errorf("Not Supported Propagation: %s", txInfo.Propagation.String()))
			}

			// 开启全局事务
			beginErr := tx.BeginWithTimeoutAndName(txInfo.TimeOut, txInfo.Name, invCtx)
			if beginErr != nil {
				return proxy.ReturnWithError(methodDesc, errors.WithStack(beginErr))
			}

			// 执行原函数
			returnValues = proxy.Invoke(methodDesc, invCtx, args)

			// error返回值校验
			errValue := returnValues[len(returnValues)-1]

			// todo 只要出错就回滚，未来可以优化一下，某些错误才回滚，某些错误的情况下，可以提交
			if errValue.IsValid() && !errValue.IsNil() {
				rollbackErr := tx.Rollback(invCtx)
				if rollbackErr != nil {
					return proxy.ReturnWithError(methodDesc, errors.WithStack(rollbackErr))
				}
				return returnValues
			}

			commitErr := tx.Commit(invCtx)
			if commitErr != nil {
				return proxy.ReturnWithError(methodDesc, errors.WithStack(commitErr))
			}

			return returnValues
		}
	}

	numField := valueOfElem.NumField()
	for i := 0; i < numField; i++ {
		t := typeOf.Field(i)
		methodName := t.Name
		f := valueOfElem.Field(i)
		// 对未赋值的函数类型进行代理
		if f.Kind() == reflect.Func && f.IsValid() && f.CanSet() {
			outNum := t.Type.NumOut() // 返回func类型的返回值个数，如果不是函数，将会panic

			// The latest return type of the method must be error.
			// 返回func类型的第i个返回值的类型，最后一个返回值必须是error
			if returnType := t.Type.Out(outNum - 1); returnType != typError {
				log.Warnf("the latest return type %s of method %q is not error", returnType, t.Name)
				continue
			}

			// 注册代理信息
			methodDescriptor := proxy.Register(proxyService, methodName)

			// do method proxy here:
			// 设置代理函数
			f.Set(reflect.MakeFunc(f.Type(), makeCallProxy(methodDescriptor, v.GetMethodTransactionInfo(methodName))))
			log.Debugf("set method [%s]", methodName)
		}
	}
}
