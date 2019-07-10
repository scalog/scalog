package discovery

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/scalog/scalog/discovery/discpb"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/pkg/address"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

func Start() {
	// read configuration
	orderAddr := viper.GetString("order-addr")
	log.Infof("%v: %v", "order-addr", orderAddr)
	// for kubernetes deployment, use k8sOrderAddr := address.NewK8sOrderAddr(orderPort)
	localOrderAddr := address.NewLocalOrderAddr(orderAddr)
	discAddr := viper.GetString("discovery-addr")
	log.Infof("%v: %v", "discovery-addr", discAddr)
	numReplica := int32(viper.GetInt("data-replication-factor"))
	log.Infof("%v: %v", "data-replication-factor", numReplica)
	// parse to get port to listen to
	addr := strings.Split(discAddr, ":")
	if len(addr) != 2 {
		log.Fatalf("Discovery address format error: %v", discAddr)
	}
	port, err := strconv.Atoi(addr[1])
	if err != nil {
		log.Fatalf("Discovery port format error%v", err)
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
	// order server
	server := NewDiscoveryServer(numReplica, localOrderAddr)
	if server == nil {
		log.Fatalf("Failed to create discovery server")
	}
	discpb.RegisterDiscoveryServer(grpcServer, server)
	server.Start()
	// serve grpc server
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf("Failed to server grpc: %v", err)
	}
}
