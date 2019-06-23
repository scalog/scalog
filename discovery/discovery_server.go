package discovery

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/scalog/scalog/discovery/discpb"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/order/orderpb"

	"google.golang.org/grpc"
)

type DiscoveryServer struct {
	// server configuration
	numReplica int32
	// server state
	clientID int32 // incremental counter to distinguish clients
	viewID   int32
	shards   map[int32]bool
	viewMu   sync.Mutex
	// ordering layer information
	orderAddr   string
	orderConn   *grpc.ClientConn
	orderClient *orderpb.Order_ReportClient
	orderMu     sync.Mutex

	viewC   map[int32]chan *discpb.View
	viewCMu sync.Mutex
}

func NewDiscoveryServer(numReplica int32, orderAddr string) *DiscoveryServer {
	ds := &DiscoveryServer{
		numReplica: numReplica,
		viewID:     -1,
		shards:     make(map[int32]bool),
		clientID:   0,
		viewC:      make(map[int32]chan *discpb.View),
	}
	var err error
	for i := 0; i < 10; i++ {
		err = ds.UpdateOrderAddr(orderAddr)
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		return nil
	}
	return ds
}

func (server *DiscoveryServer) Start() {
	go server.subscribe()
}

func (server *DiscoveryServer) UpdateOrderAddr(addr string) error {
	server.orderMu.Lock()
	defer server.orderMu.Unlock()
	server.orderAddr = addr
	opts := []grpc.DialOption{grpc.WithInsecure()}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return fmt.Errorf("Dial peer %v failed: %v", addr, err)
	}
	server.orderConn = conn
	orderClient := orderpb.NewOrderClient(conn)
	orderReportClient, err := orderClient.Report(context.Background())
	if err != nil {
		return fmt.Errorf("Create replicate client to %v failed: %v", addr, err)
	}
	server.orderClient = &orderReportClient
	return nil
}

func (server *DiscoveryServer) subscribe() {
	for {
		entry, err := (*server.orderClient).Recv()
		if err != nil {
			log.Fatalf("%v", err)
		}
		// check if there is any update on view
		server.viewMu.Lock()
		if entry.ViewID == server.viewID {
			server.viewMu.Unlock()
			continue
		}
		server.viewMu.Unlock()
		// make sure the view id change is incremental
		if entry.ViewID-server.viewID != 1 {
			log.Errorf("ViewID is not incremental: current %v, received %v", server.viewID, entry.ViewID)
			continue
		}
		// update view stored as discovery server state
		server.viewMu.Lock()
		server.viewID = entry.ViewID
		if entry.CommittedCut != nil {
			for s := range entry.CommittedCut.Cut {
				server.shards[s/server.numReplica] = true
			}
		}
		if entry.FinalizeShards != nil {
			for _, s := range entry.FinalizeShards.ShardIDs {
				server.shards[s] = false
			}
		}
		server.viewMu.Unlock()
		// get view in discpb.View format
		view := server.getView()
		// send the view through viewC channels
		server.viewCMu.Lock()
		for _, vc := range server.viewC {
			vc <- view
		}
		server.viewCMu.Unlock()
	}
}

func (server *DiscoveryServer) getView() *discpb.View {
	view := &discpb.View{ViewID: server.viewID}
	liveShards := make([]int32, 0)
	finalizedShards := make([]int32, 0)
	server.viewMu.Lock()
	for s, o := range server.shards {
		if o {
			liveShards = append(liveShards, s)
		} else {
			finalizedShards = append(finalizedShards, s)
		}
	}
	server.viewMu.Unlock()
	view.LiveShards = liveShards
	view.FinalizedShards = finalizedShards
	return view
}
