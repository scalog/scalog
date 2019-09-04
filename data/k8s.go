package data

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/scalog/scalog/data/datapb"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/pkg/address"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

func StartK8s() {
	// read configuration
	shardGroup, replicaID, shardID := parseDataPodName(viper.GetString("name"))
	viper.Set("sid", shardID)
	viper.Set("rid", replicaID)
	viper.Set("shardGroup", shardGroup)
	sid := int32(viper.GetInt("sid"))
	rid := int32(viper.GetInt("rid"))
	numReplica := int32(viper.GetInt("data-replication-factor"))
	batchingInterval, err := time.ParseDuration(viper.GetString("data-batching-interval"))
	if err != nil {
		log.Fatalf("Failed to parse data-batching-interval: %v", err)
	}
	port := uint16(viper.GetInt("data-port"))
	orderPort := uint16(viper.GetInt("order-port"))
	// print log
	log.Infof("%v: %v", "sid", sid)
	log.Infof("%v: %v", "rid", rid)
	log.Infof("%v: %v", "data-replication-factor", numReplica)
	log.Infof("%v: %v", "data-batching-interval", batchingInterval)
	log.Infof("%v: %v", "data-port", port)
	// get ordering layer
	k8sOrderAddr := address.NewK8sOrderAddr(orderPort)
	k8sDataAddr := address.NewK8sDataAddr(port)
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
	// data server
	server := NewDataServer(rid, sid, numReplica, batchingInterval, k8sDataAddr, k8sOrderAddr)
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

func parseDataPodName(podName string) (string, int, int) {
	splitPodName := strings.Split(podName, "-")
	shardGroup := strings.Join(splitPodName[:len(splitPodName)-1], "-")
	shardID, err := strconv.Atoi(splitPodName[len(splitPodName)-2])
	if err != nil {
		return "", -1, -1
	}
	replicaID, err := strconv.Atoi(splitPodName[len(splitPodName)-1])
	if err != nil {
		replicaID = -1
		shardGroup = ""
		shardID = -1
	}
	return shardGroup, replicaID, shardID
}
