package data

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/scalog/scalog/data/datapb"
	log "github.com/scalog/scalog/logger"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

func Start() {
	// read configuration
	sid := int32(viper.GetInt("sid"))
	log.Infof("%v: %v", "sid", sid)
	rid := int32(viper.GetInt("rid"))
	log.Infof("%v: %v", "rid", rid)
	numReplica := int32(viper.GetInt("data-replication-factor"))
	log.Infof("%v: %v", "data-replication-factor", numReplica)
	batchingInterval, err := time.ParseDuration(viper.GetString("data-batching-interval"))
	if err != nil {
		log.Fatalf("Failed to parse ordering-batching-interval: %v", err)
	}
	log.Infof("%v: %v", "data-batching-interval", batchingInterval)
	basePort := int32(viper.GetInt("data-port"))
	port := basePort + (sid * numReplica) + rid
	log.Infof("%v: %v", "data-port", port)
	orderAddr := viper.GetString("order-addr")
	log.Infof("%v: %v", "order-addr", orderAddr)
	peerList := make([]string, numReplica)
	for i := int32(0); i < numReplica; i++ {
		peerList[int(i)] = fmt.Sprintf("127.0.0.1:%v", basePort+(sid*numReplica)+i)
	}
	peers := strings.Join(peerList, ",")
	log.Infof("%v: %v", "data-peers", peers)
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
	// health server
	healthServer := health.NewServer()
	healthServer.Resume()
	healthgrpc.RegisterHealthServer(grpcServer, healthServer)
	// data server
	server := NewDataServer(rid, sid, numReplica, batchingInterval, peers, orderAddr)
	if server == nil {
		log.Fatalf("Failed to create data server")
	}
	datapb.RegisterDataServer(grpcServer, server)
	go func() {
		err := grpcServer.Serve(lis)
		if err != nil {
			log.Fatalf("%v", err)
		}
	}()
	server.Start()
	for {
		time.Sleep(time.Second)
	}

}
