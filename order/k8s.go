package order

import (
	"fmt"
	"net"
	"time"

	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/order/orderpb"
	"github.com/scalog/scalog/pkg/kube"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

func StartK8s() {
	// read configuration
	oid := int32(viper.GetInt("oid"))
	numReplica := int32(viper.GetInt("order-replication-factor"))
	dataNumReplica := int32(viper.GetInt("data-replication-factor"))
	batchingInterval, err := time.ParseDuration(viper.GetString("order-batching-interval"))
	if err != nil {
		log.Fatalf("Failed to parse order-batching-interval: %v", err)
	}
	port := int32(viper.GetInt("order-port"))
	raftPort := int32(viper.GetInt("raft-port"))
	namespace := viper.GetString("namespace")
	// print log
	log.Infof("%v: %v", "oid", oid)
	log.Infof("order-port: %v", port)
	log.Infof("Starting order server %v at 0.0.0.0:%v", oid, port)
	log.Infof("replication-factor: %v", numReplica)
	log.Infof("order-batching-interval: %v", batchingInterval)
	// get raft peer list
	pods := kube.GetShardPods(kube.InitKubernetesClient(), "app=scalog-order", int(numReplica), namespace)
	peerList := make([]string, numReplica)
	for i := int32(0); i < numReplica; i++ {
		peerList[int(i)] = fmt.Sprintf("http://%v:%v", pods.Items[i].Status.PodIP, raftPort)
	}
	// listen to the port
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", port))
	if err != nil {
		log.Fatalf("Failed to listen to port %v: %v", port, err)
	}
	grpcServer := grpc.NewServer()
	// server should register all the services manually
	// use empty service name for all scalog services' health status,
	// see https://github.com/grpc/grpc/blob/master/doc/health-checking.md for more
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthgrpc.HealthCheckResponse_SERVING)
	healthgrpc.RegisterHealthServer(grpcServer, healthServer)
	// order server
	server := NewOrderServer(oid, numReplica, dataNumReplica, batchingInterval, peerList)
	orderpb.RegisterOrderServer(grpcServer, server)
	// serve grpc server
	go func() {
		err = grpcServer.Serve(lis)
		if err != nil {
			log.Fatalf("Failed to server grpc: %v", err)
		}
	}()
	server.Start()
	for {
		time.Sleep(time.Second)
	}
}
