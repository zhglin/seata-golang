package main

import (
	"fmt"
	"github.com/opentrx/seata-golang/v2/pkg/util/uuid"
	"net"
	"os"

	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"

	"github.com/opentrx/seata-golang/v2/pkg/apis"
	"github.com/opentrx/seata-golang/v2/pkg/tc/config"
	_ "github.com/opentrx/seata-golang/v2/pkg/tc/metrics"
	"github.com/opentrx/seata-golang/v2/pkg/tc/server"
	_ "github.com/opentrx/seata-golang/v2/pkg/tc/storage/driver/in_memory"
	_ "github.com/opentrx/seata-golang/v2/pkg/tc/storage/driver/mysql"
	_ "github.com/opentrx/seata-golang/v2/pkg/tc/storage/driver/pgsql"
	"github.com/opentrx/seata-golang/v2/pkg/util/log"
)

// https://www.bilibili.com/video/BV1oz411e72T
func main() {
	app := &cli.App{
		Commands: []*cli.Command{
			{
				Name:  "start",
				Usage: "start seata golang tc server",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "config",
						Aliases: []string{"c"},
						Usage:   "Load configuration from `FILE`",
					},
					&cli.StringFlag{
						Name:    "serverNode",
						Aliases: []string{"n"},
						Value:   "1",
						Usage:   "server node id, such as 1, 2, 3. default is 1",
					},
				},
				Action: func(c *cli.Context) error {
					configPath := c.String("config")
					serverNode := c.Int64("serverNode")

					// 加载配置文件
					config, err := resolveConfiguration(configPath)

					// 设置id生成器的workerId
					uuid.Init(serverNode)
					log.Init(config.Log.LogPath, config.Log.LogLevel)

					address := fmt.Sprintf(":%v", config.Server.Port)
					lis, err := net.Listen("tcp", address)
					if err != nil {
						log.Fatalf("failed to listen: %v", err)
					}

					s := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(config.GetEnforcementPolicy()),
						grpc.KeepaliveParams(config.GetServerParameters()))

					tc := server.NewTransactionCoordinator(config)
					apis.RegisterTransactionManagerServiceServer(s, tc)
					apis.RegisterResourceManagerServiceServer(s, tc)

					if err := s.Serve(lis); err != nil {
						log.Fatalf("failed to serve: %v", err)
					}
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Error(err)
	}
}

// 加载配置文件
func resolveConfiguration(configPath string) (*config.Configuration, error) {
	var configurationPath string

	// 配置文件路径
	if configPath != "" {
		configurationPath = configPath
	} else if os.Getenv("SEATA_CONFIGURATION_PATH") != "" {
		configurationPath = os.Getenv("SEATA_CONFIGURATION_PATH")
	}

	if configurationPath == "" {
		return nil, fmt.Errorf("configuration path unspecified")
	}

	fp, err := os.Open(configurationPath)
	if err != nil {
		return nil, err
	}

	defer fp.Close()

	config, err := config.Parse(fp)
	if err != nil {
		return nil, fmt.Errorf("error parsing %s: %v", configurationPath, err)
	}

	return config, nil
}
