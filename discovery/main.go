package discovery

import (
	"fmt"
	"net"
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
	orderPort := uint16(viper.GetInt("order-port"))
	// for kubernetes deployment, use k8sOrderAddr := address.NewK8sOrderAddr(orderPort)
	localOrderAddr := address.NewLocalOrderAddr(orderPort)
	port := int16(viper.GetInt("disc-port"))
	numReplica := int32(viper.GetInt("data-replication-factor"))
	log.Infof("%v: %v", "data-replication-factor", numReplica)
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
