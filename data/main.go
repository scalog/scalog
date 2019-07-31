package data

import (
	"fmt"
	"net"
	"time"

	"github.com/scalog/scalog/data/datapb"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/pkg/address"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

func Start() {
	sid := int32(viper.GetInt("sid"))
	log.Infof("%v: %v", "sid", sid)
	rid := int32(viper.GetInt("rid"))
	log.Infof("%v: %v", "rid", rid)
	StartData(sid, rid)
}

func StartData(sid, rid int32) {
	// read configuration
	numReplica := int32(viper.GetInt("data-replication-factor"))
	log.Infof("%v: %v", "data-replication-factor", numReplica)
	batchingInterval, err := time.ParseDuration(viper.GetString("data-batching-interval"))
	if err != nil {
		log.Fatalf("Failed to parse ordering-batching-interval: %v", err)
	}
	log.Infof("%v: %v", "data-batching-interval", batchingInterval)
	basePort := uint16(viper.GetInt("data-port"))
	port := basePort + uint16(sid*numReplica+rid)
	log.Infof("%v: %v", "data-port", port)
	orderPort := uint16(viper.GetInt("order-port"))
	// for kubernetes deployment, use k8sOrderAddr := address.NewK8sOrderAddr(orderPort)
	localOrderAddr := address.NewLocalOrderAddr(orderPort)
	localDataAddr := address.NewLocalDataAddr(numReplica, basePort)
	// listen to the port
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", port))
	if err != nil {
		log.Fatalf("Failed to listen to port %v: %v", port, err)
	}
	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
		}),
	)
	// server should register all the services manually
	// use empty service name for all scalog services' health status,
	// see https://github.com/grpc/grpc/blob/master/doc/health-checking.md for more
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthgrpc.HealthCheckResponse_SERVING)
	healthgrpc.RegisterHealthServer(grpcServer, healthServer)
	// data server
	server := NewDataServer(rid, sid, numReplica, batchingInterval, localDataAddr, localOrderAddr)
	if server == nil {
		log.Fatalf("Failed to create data server")
	}
	datapb.RegisterDataServer(grpcServer, server)
	go func() {
		err = grpcServer.Serve(lis)
		if err != nil {
			log.Fatalf("%v", err)
		}
	}()
	server.Start()
	for {
		time.Sleep(time.Second)
	}
}
