package order

import (
	"fmt"
	"net"
	"time"

	log "github.com/scalog/scalogger/logger"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

func Start() {
	numReplica := int32(viper.GetInt("order-replication-factor"))
	dataNumReplica := int32(viper.GetInt("data-replication-factor"))
	batchingInterval, err := time.ParseDuration(viper.GetString("order-batching-interval"))
	if err != nil {
		log.Fatalf("Failed to parse order-batching-interval: %v", err)
	}
	port := int32(viper.GetInt("order-port"))
	index := int32(viper.GetInt("id"))
	log.Infof("Starting order server %v at 0.0.0.0:%v", index, port)
	log.Infof("replication-factor: %v", numReplica)
	log.Infof("order-batching-interval: %v", batchingInterval)

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", port))
	if err != nil {
		log.Fatalf("Failed to listen to port %v: %v", port, err)
	}
	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
		}),
	)

	healthServer := health.NewServer()
	healthServer.Resume()
	healthgrpc.RegisterHealthServer(grpcServer, healthServer)

	server := NewOrderServer(index, numReplica, dataNumReplica, batchingInterval)
	go server.Start()

	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf("Failed to server grpc: %v", err)
	}
}
