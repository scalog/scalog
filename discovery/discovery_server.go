package discovery

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/scalog/scalog/discovery/discpb"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/order/orderpb"
	"github.com/scalog/scalog/pkg/address"

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
	orderAddr   address.OrderAddr
	orderConn   *grpc.ClientConn
	orderClient *orderpb.Order_ReportClient
	orderMu     sync.Mutex

	viewC   map[int32]chan *discpb.View
	viewCMu sync.Mutex
}

func NewDiscoveryServer(numReplica int32, orderAddr address.OrderAddr) *DiscoveryServer {
	ds := &DiscoveryServer{
		numReplica: numReplica,
		viewID:     -1,
		shards:     make(map[int32]bool),
		clientID:   0,
		viewC:      make(map[int32]chan *discpb.View),
		orderAddr:  orderAddr,
	}
	var err error
	for i := 0; i < 100; i++ {
		err = ds.UpdateOrder()
		if err == nil {
			break
		}
		log.Warningf("%v", err)
		time.Sleep(time.Second)
	}
	if err != nil {
		return nil
	}
	return ds
}

func (s *DiscoveryServer) Start() {
	go s.subscribe()
}

func (s *DiscoveryServer) UpdateOrderAddr(addr string) error {
	s.orderMu.Lock()
	defer s.orderMu.Unlock()
	if s.orderConn != nil {
		s.orderConn.Close()
		s.orderConn = nil
	}
	opts := []grpc.DialOption{grpc.WithInsecure(), grpc.WithBlock()}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return fmt.Errorf("Dial peer %v failed: %v", addr, err)
	}
	s.orderConn = conn
	orderClient := orderpb.NewOrderClient(conn)
	orderReportClient, err := orderClient.Report(context.Background())
	if err != nil {
		return fmt.Errorf("Create replicate client to %v failed: %v", addr, err)
	}
	s.orderClient = &orderReportClient
	return nil
}

func (s *DiscoveryServer) UpdateOrder() error {
	addr := s.orderAddr.Get()
	if len(addr) == 0 {
		return fmt.Errorf("Wrong order-addr format: %v", addr)
	}
	return s.UpdateOrderAddr(addr)
}

func (s *DiscoveryServer) subscribe() {
	for {
		entry, err := (*s.orderClient).Recv()
		if err != nil {
			log.Errorf("%v", err)
			for {
				err := s.UpdateOrder()
				if err == nil {
					break
				}
				time.Sleep(time.Millisecond)
			}
			continue
		}
		log.Debugf("Disc: %v", entry)
		// check if there is any update on view
		s.viewMu.Lock()
		if entry.ViewID == s.viewID {
			s.viewMu.Unlock()
			continue
		}
		s.viewMu.Unlock()
		// make sure the view id change is incremental
		if s.viewID >= 0 && entry.ViewID-s.viewID != 1 {
			log.Errorf("ViewID is not incremental: current %v, received %v", s.viewID, entry.ViewID)
			for { // TODO: the failure handling should be polished
				err := s.UpdateOrder()
				if err == nil {
					break
				}
				time.Sleep(time.Second)
			}
			continue
		}
		// update view stored as discovery server state
		s.viewMu.Lock()
		s.viewID = entry.ViewID
		if entry.CommittedCut != nil {
			for c := range entry.CommittedCut.Cut {
				s.shards[c/s.numReplica] = true
			}
		}
		if entry.FinalizeShards != nil {
			for _, sid := range entry.FinalizeShards.ShardIDs {
				s.shards[sid] = false
			}
		}
		s.viewMu.Unlock()
		// get view in discpb.View format
		view := s.getView()
		log.Debugf("Disc: %v", view)
		// send the view through viewC channels
		s.viewCMu.Lock()
		for _, vc := range s.viewC {
			vc <- view
		}
		s.viewCMu.Unlock()
	}
}

func (s *DiscoveryServer) getView() *discpb.View {
	view := &discpb.View{ViewID: s.viewID}
	liveShards := make([]int32, 0)
	finalizedShards := make([]int32, 0)
	s.viewMu.Lock()
	for sid, o := range s.shards {
		if o {
			liveShards = append(liveShards, sid)
		} else {
			finalizedShards = append(finalizedShards, sid)
		}
	}
	s.viewMu.Unlock()
	view.LiveShards = liveShards
	view.FinalizedShards = finalizedShards
	return view
}
