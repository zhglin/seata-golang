package client

import (
	"log"

	"google.golang.org/grpc"

	"github.com/opentrx/seata-golang/v2/pkg/apis"
	"github.com/opentrx/seata-golang/v2/pkg/client/config"
	"github.com/opentrx/seata-golang/v2/pkg/client/rm"
	"github.com/opentrx/seata-golang/v2/pkg/client/tcc"
	"github.com/opentrx/seata-golang/v2/pkg/client/tm"
)

// Init init resource manager，init transaction manager, expose a port to listen tc
// call back request.
// 初始化资源管理器，初始化事务管理器，公开一个端口来监听tc回调请求。
func Init(config *config.Configuration) {
	conn, err := grpc.Dial(config.ServerAddressing,
		grpc.WithInsecure(),
		grpc.WithKeepaliveParams(config.GetClientParameters()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	resourceManagerClient := apis.NewResourceManagerServiceClient(conn)
	transactionManagerClient := apis.NewTransactionManagerServiceClient(conn)

	// 分支事务链接
	rm.InitResourceManager(config.Addressing, resourceManagerClient)
	// 初始化tc链接
	tm.InitTransactionManager(config.Addressing, transactionManagerClient)
	rm.RegisterTransactionServiceServer(tcc.GetTCCResourceManager())
}
