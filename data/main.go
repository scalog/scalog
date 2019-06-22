package data

import (
	"fmt"
	"net"
	"strconv"
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
	orderAddr := viper.GetString("order-addr")
	log.Infof("%v: %v", "order-addr", orderAddr)
	peers := viper.GetString("data-peers")
	log.Infof("%v: %v", "data-peers", peers)
	// parse peers to get port to listen to
	ps := strings.Split(peers, ",")
	if int32(len(ps)) != numReplica {
		log.Fatalf("The number of peers in peer list doesn't match the number of replicas: %v vs %v", len(ps), numReplica)
	}
	addr := strings.Split(ps[rid], ":")
	if len(addr) != 2 {
		log.Fatalf("Peer address format error: %v", ps[rid])
	}
	port, err := strconv.Atoi(addr[1])
	if err != nil {
		log.Fatalf("Peer port format error%v", err)
	}
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
	go grpcServer.Serve(lis)
	server.Start()
	for {
		time.Sleep(time.Second)
	}

}
