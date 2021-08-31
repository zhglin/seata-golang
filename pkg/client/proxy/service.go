package proxy

import (
	"context"
	"reflect"
	"sync"
	"unicode"
	"unicode/utf8"

	ctx "github.com/opentrx/seata-golang/v2/pkg/client/base/context"
	"github.com/opentrx/seata-golang/v2/pkg/util/log"
)

var (
	// Precompute the reflect type for error. Can't use error directly
	// because Typeof takes an empty interface value. This is annoying.
	typeOfError = reflect.TypeOf((*error)(nil)).Elem()

	// serviceDescriptorMap, string -> *ServiceDescriptor
	// 所有的代理信息
	serviceDescriptorMap = sync.Map{}
)

// MethodDescriptor 被代理的函数信息
type MethodDescriptor struct {
	Method           reflect.Method
	CallerValue      reflect.Value  // 调用方struct
	CtxType          reflect.Type   // context.Context 参数
	ArgsType         []reflect.Type // 参数类型
	ArgsNum          int            // 参数个数
	ReturnValuesType []reflect.Type // 返回值类型
	ReturnValuesNum  int            // 返回值个数
}

// ServiceDescriptor 被代理的struct以及函数信息
type ServiceDescriptor struct {
	Name         string
	ReflectType  reflect.Type
	ReflectValue reflect.Value
	Methods      sync.Map //string -> *MethodDescriptor
}

// Register 注册需要被代理的服务以及函数
func Register(service interface{}, methodName string) *MethodDescriptor {
	serviceType := reflect.TypeOf(service)
	serviceValue := reflect.ValueOf(service)
	svcName := reflect.Indirect(serviceValue).Type().Name() // 类型在自身包内的类型名

	// 存储被代理的strut信息
	svcDesc, _ := serviceDescriptorMap.LoadOrStore(svcName, &ServiceDescriptor{
		Name:         svcName,
		ReflectType:  serviceType,
		ReflectValue: serviceValue,
		Methods:      sync.Map{},
	})

	// 被代理的函数
	svcDescriptor := svcDesc.(*ServiceDescriptor)
	methodDesc, methodExist := svcDescriptor.Methods.Load(methodName)
	if methodExist {
		methodDescriptor := methodDesc.(*MethodDescriptor)
		return methodDescriptor
	}

	method, methodFounded := serviceType.MethodByName(methodName) // 被代理的结构体类型中查找函数
	if methodFounded {
		methodDescriptor := describeMethod(method)
		if methodDescriptor != nil {
			methodDescriptor.CallerValue = serviceValue               // 设置调用方
			svcDescriptor.Methods.Store(methodName, methodDescriptor) // 设置
			return methodDescriptor
		}
	}
	return nil
}

// describeMethod
// might return nil when method is not exported or some other error
// 校验解析method
func describeMethod(method reflect.Method) *MethodDescriptor {
	methodType := method.Type
	methodName := method.Name
	inNum := methodType.NumIn()   // 参数个数
	outNum := methodType.NumOut() // 返回值个数

	// Method must be exported.
	// 方法必须导出。
	if method.PkgPath != "" {
		return nil
	}

	var (
		ctxType                    reflect.Type
		argsType, returnValuesType []reflect.Type
	)

	// 参数校验
	for index := 1; index < inNum; index++ {
		// 参数名
		if methodType.In(index).String() == "context.Context" {
			ctxType = methodType.In(index)
		}
		argsType = append(argsType, methodType.In(index))
		// need not be a pointer.
		// 参数不是可导出的
		if !isExportedOrBuiltinType(methodType.In(index)) {
			log.Errorf("argument type of method %q is not exported %v", methodName, methodType.In(index))
			return nil
		}
	}

	// returnValuesType 返回值校验
	for num := 0; num < outNum; num++ {
		returnValuesType = append(returnValuesType, methodType.Out(num))
		// need not be a pointer.
		if !isExportedOrBuiltinType(methodType.Out(num)) {
			log.Errorf("reply type of method %s not exported{%v}", methodName, methodType.Out(num))
			return nil
		}
	}

	return &MethodDescriptor{
		Method:           method,
		CtxType:          ctxType,
		ArgsType:         argsType,
		ArgsNum:          inNum,
		ReturnValuesType: returnValuesType,
		ReturnValuesNum:  outNum,
	}
}

// Is this an exported - upper case - name
// 首字母是否大写
func isExported(name string) bool {
	s, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(s)
}

// Is this type exported or a builtin?
// 这个类型是导出的还是内置的?
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	// PkgPath将是非空的，即使对于导出的类型，所以我们也需要检查类型名。
	return isExported(t.Name()) || t.PkgPath() == ""
}

// Invoke 执行被代理的函数
func Invoke(methodDesc *MethodDescriptor, ctx *ctx.RootContext, args []interface{}) []reflect.Value {

	in := []reflect.Value{methodDesc.CallerValue} // 接收者 struct

	for i := 0; i < len(args); i++ {
		t := reflect.ValueOf(args[i])
		if methodDesc.ArgsType[i].String() == "context.Context" {
			t = SuiteContext(methodDesc, ctx)
		}
		if !t.IsValid() { // 没值 构建一个
			at := methodDesc.ArgsType[i]
			if at.Kind() == reflect.Ptr {
				at = at.Elem()
			}
			t = reflect.New(at)
		}
		in = append(in, t)
	}

	returnValues := methodDesc.Method.Func.Call(in)

	return returnValues
}

// SuiteContext 构造rootContext
func SuiteContext(methodDesc *MethodDescriptor, ctx context.Context) reflect.Value {
	if contextValue := reflect.ValueOf(ctx); contextValue.IsValid() {
		return contextValue
	}
	return reflect.Zero(methodDesc.CtxType)
}

// ReturnWithError 返回最后一个参数error
func ReturnWithError(methodDesc *MethodDescriptor, err error) []reflect.Value {
	var result = make([]reflect.Value, 0)
	// 非最后一个参数返回nil
	for i := 0; i < methodDesc.ReturnValuesNum-1; i++ {
		result = append(result, reflect.Zero(methodDesc.ReturnValuesType[i]))
	}
	// 最后一个参数返回error
	result = append(result, reflect.ValueOf(err))
	return result
}
