package data

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/scalog/scalog/data/datapb"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/order/orderpb"
	"github.com/scalog/scalog/pkg/address"
	"github.com/scalog/scalog/storage"
	"google.golang.org/grpc"
)

type DataServer struct {
	// data s configurations
	shardID          int32
	replicaID        int32
	numReplica       int32
	batchingInterval time.Duration
	// s state
	clientID         int32 // incremental counter to distinguish clients
	viewID           int32
	localCut         []int64
	localCutMu       sync.Mutex
	prevCommittedCut *orderpb.CommittedCut
	// ordering layer information
	orderAddr   address.OrderAddr
	orderConn   *grpc.ClientConn
	orderClient *orderpb.Order_ReportClient
	orderMu     sync.RWMutex
	// peer information
	dataAddr    address.DataAddr
	peerConns   []*grpc.ClientConn
	peerClients []*datapb.Data_ReplicateClient
	peerDoneC   []chan interface{}
	// channels used to communate with clients, peers, and ordering layer
	appendC        chan *datapb.Record
	replicateC     chan *datapb.Record
	replicateSendC []chan *datapb.Record
	ackC           chan *datapb.Ack
	ackSendC       map[int32]chan *datapb.Ack
	ackSendCMu     sync.RWMutex
	subC           map[int32]chan *datapb.Record
	subCMu         sync.RWMutex

	storage *storage.Storage

	wait   map[int64]chan *datapb.Ack
	waitMu sync.RWMutex

	committedEntryC chan *orderpb.CommittedEntry

	// stores records directly sent to this replica
	records   map[int64]*datapb.Record
	recordsMu sync.Mutex
}

func NewDataServer(replicaID, shardID, numReplica int32, batchingInterval time.Duration, dataAddr address.DataAddr, orderAddr address.OrderAddr) *DataServer {
	var err error
	s := &DataServer{
		replicaID:        replicaID,
		shardID:          shardID,
		clientID:         0,
		viewID:           0,
		numReplica:       numReplica,
		batchingInterval: batchingInterval,
		orderAddr:        orderAddr,
		dataAddr:         dataAddr,
	}
	s.localCut = make([]int64, numReplica)
	for i := 0; i < int(numReplica); i++ {
		s.localCut[i] = 0
	}
	// initialize basic data structures
	s.committedEntryC = make(chan *orderpb.CommittedEntry, 4096)
	s.ackSendC = make(map[int32]chan *datapb.Ack)
	s.subC = make(map[int32]chan *datapb.Record)
	s.ackC = make(chan *datapb.Ack, 4096)
	s.appendC = make(chan *datapb.Record, 4096)
	s.replicateC = make(chan *datapb.Record, 4096)
	s.replicateSendC = make([]chan *datapb.Record, numReplica)
	s.peerDoneC = make([]chan interface{}, numReplica)
	s.wait = make(map[int64]chan *datapb.Ack)
	s.prevCommittedCut = &orderpb.CommittedCut{}
	s.records = make(map[int64]*datapb.Record)
	path := fmt.Sprintf("log/storage-%v-%v", shardID, replicaID) // TODO configure path
	segLen := int32(1000)                                        // TODO configurable segment length
	storage, err := storage.NewStorage(path, replicaID, numReplica, segLen)
	if err != nil {
		log.Fatalf("Create storage failed: %v", err)
	}
	s.storage = storage
	for i := int32(0); i < numReplica; i++ {
		if i != replicaID {
			s.replicateSendC[i] = make(chan *datapb.Record, 4096)
		}
	}
	for i := 0; i < 100; i++ {
		err = s.UpdateOrder()
		if err == nil {
			break
		}
		log.Warningf("%v", err)
		time.Sleep(time.Second)
	}
	if err != nil {
		log.Errorf("%v", err)
		return nil
	}
	return s
}

func (s *DataServer) UpdateOrderAddr(addr string) error {
	s.orderMu.Lock()
	defer s.orderMu.Unlock()
	if s.orderConn != nil {
		s.orderConn.Close()
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

func (s *DataServer) UpdateOrder() error {
	addr := s.orderAddr.Get()
	if addr == "" {
		return fmt.Errorf("Wrong order-addr format: %v", addr)
	}
	return s.UpdateOrderAddr(addr)
}

// UpdatePeers updates the peer list of the shard. It should be called only at
// the initialization phase of the s.
// TODO make the list updatable when running
func (s *DataServer) ConnPeers() error {
	// create connections with peers
	s.peerConns = make([]*grpc.ClientConn, s.numReplica)
	s.peerClients = make([]*datapb.Data_ReplicateClient, s.numReplica)
	for i := int32(0); i < s.numReplica; i++ {
		if i == s.replicaID {
			continue
		}
		err := s.connectToPeer(i)
		if err != nil {
			log.Errorf("%v", err)
			return err
		}
		done := make(chan interface{})
		sendC := s.replicateSendC[i]
		client := s.peerClients[i]
		s.peerDoneC[i] = done
		go s.replicateRecords(done, sendC, client)
	}
	return nil
}

func (s *DataServer) Start() {
	for i := 0; i < 100; i++ {
		err := s.ConnPeers()
		if err != nil {
			log.Errorf("%v", err)
			continue
		}
		go s.processAppend()
		go s.processReplicate()
		go s.processAck()
		go s.processCommittedEntry()
		go s.reportLocalCut()
		go s.receiveCommittedCut()
		return
	}
	log.Errorf("Error creating data s sid=%v,rid=%v", s.shardID, s.replicaID)
}

func (s *DataServer) connectToPeer(peer int32) error {
	// do not connect to the node itself
	if peer == s.replicaID {
		return nil
	}
	// close existing network connections if they exist
	if s.peerConns[peer] != nil {
		s.peerConns[peer].Close()
		s.peerConns[peer] = nil
	}
	if s.peerDoneC[peer] != nil {
		close(s.peerDoneC[peer])
		s.peerDoneC[peer] = nil
	}
	// build connections to peers
	opts := []grpc.DialOption{grpc.WithInsecure(), grpc.WithBlock()}
	var conn *grpc.ClientConn
	var err error
	for i := 0; i < 100; i++ {
		conn, err = grpc.Dial(s.dataAddr.Get(s.shardID, peer), opts...)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return fmt.Errorf("Dial peer %v failed: %v", s.dataAddr.Get(s.shardID, peer), err)
	}
	s.peerConns[peer] = conn
	dataClient := datapb.NewDataClient(conn)
	callOpts := []grpc.CallOption{}
	replicateSendClient, err := dataClient.Replicate(context.Background(), callOpts...)
	if err != nil {
		return fmt.Errorf("Create replicate client to %v failed: %v", s.dataAddr.Get(s.shardID, peer), err)
	}
	s.peerClients[peer] = &replicateSendClient
	return nil
}

func (s *DataServer) replicateRecords(done <-chan interface{}, ch chan *datapb.Record, client *datapb.Data_ReplicateClient) {
	for {
		select {
		case record := <-ch:
			log.Debugf("Data %v,%v send: %v", s.shardID, s.replicaID, record)
			err := (*client).Send(record)
			if err != nil {
				log.Errorf("Send record error: %v", err)
			}
		case <-done:
			return
		}
	}
}

// processAppend sends records to replicateC and replicates them to peers
func (s *DataServer) processAppend() {
	for record := range s.appendC {
		record.LocalReplicaID = s.replicaID
		record.ShardID = s.shardID
		s.replicateC <- record
		for i, c := range s.replicateSendC {
			if int32(i) != s.replicaID {
				log.Debugf("Data forward to %v: %v", i, record)
				c <- record
			}
		}
	}
}

// processReplicate writes records to local storage
func (s *DataServer) processReplicate() {
	for record := range s.replicateC {
		log.Debugf("Data %v,%v process: %v", s.shardID, s.replicaID, record)
		lsn, err := s.storage.WriteToPartition(record.LocalReplicaID, record.Record)
		if err != nil {
			log.Fatalf("Write to storage failed: %v", err)
		}
		s.recordsMu.Lock()
		s.records[lsn] = record
		s.recordsMu.Unlock()
		s.localCutMu.Lock()
		s.localCut[record.LocalReplicaID] = lsn + 1
		s.localCutMu.Unlock()
	}
}

func (s *DataServer) processAck() {
	for ack := range s.ackC {
		// send to ack channel
		s.ackSendCMu.RLock()
		c, ok := s.ackSendC[ack.ClientID]
		s.ackSendCMu.RUnlock()
		if ok {
			c <- ack
		}
		// send individual ack message if requested to block
		id := int64(ack.ClientID)<<32 + int64(ack.ClientSN)
		s.waitMu.RLock()
		c, ok = s.wait[id]
		s.waitMu.RUnlock()
		if ok {
			c <- ack
		}
	}
}

func (s *DataServer) reportLocalCut() {
	tick := time.NewTicker(s.batchingInterval)
	for range tick.C {
		lcs := &orderpb.LocalCuts{}
		lcs.Cuts = make([]*orderpb.LocalCut, 1)
		lcs.Cuts[0] = &orderpb.LocalCut{
			ShardID:        s.shardID,
			LocalReplicaID: s.replicaID,
		}
		s.localCutMu.Lock()
		lcs.Cuts[0].Cut = make([]int64, len(s.localCut))
		copy(lcs.Cuts[0].Cut, s.localCut)
		s.localCutMu.Unlock()
		log.Debugf("Data report: %v", lcs)
		err := (*s.orderClient).Send(lcs)
		if err != nil {
			log.Errorf("%v", err)
		}
	}
}

func (s *DataServer) receiveCommittedCut() {
	for {
		e, err := (*s.orderClient).Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatalf("Receive from ordering layer error: %v", err)
		}
		s.committedEntryC <- e
	}
}

func (s *DataServer) processCommittedEntry() {
	for entry := range s.committedEntryC {
		if entry.CommittedCut != nil {
			startReplicaID := s.shardID * s.numReplica
			startGSN := entry.CommittedCut.StartGSN
			// compute startGSN using the number of records stored
			// in shards with smaller ids
			for rid, lsn := range entry.CommittedCut.Cut {
				if rid < startReplicaID {
					diff := lsn
					if l, ok := s.prevCommittedCut.Cut[rid]; ok {
						diff = lsn - l
					}
					if diff > 0 {
						startGSN += diff
					}
				}
			}
			// assign gsn to records in my shard
			for i := int32(0); i < s.numReplica; i++ {
				rid := startReplicaID + i
				lsn := entry.CommittedCut.Cut[rid]
				diff := int32(lsn)
				start := int64(0)
				if l, ok := s.prevCommittedCut.Cut[rid]; ok {
					start = l
					diff = int32(lsn - l)
				}
				if diff > 0 {
					err := s.storage.Assign(i, start, diff, startGSN)
					if err != nil {
						log.Errorf("Assign GSN to storage error: %v", err)
						continue
					}
					if i == s.replicaID {
						for j := int32(0); j < diff; j++ {
							s.recordsMu.Lock()
							record, ok := s.records[start+int64(j)]
							if ok {
								delete(s.records, start+int64(j))
							}
							s.recordsMu.Unlock()
							if !ok {
								continue
							}
							ack := &datapb.Ack{
								ClientID:       record.ClientID,
								ClientSN:       record.ClientSN,
								ShardID:        s.shardID,
								LocalReplicaID: s.replicaID,
								ViewID:         s.viewID,
								GlobalSN:       startGSN + int64(j),
							}
							s.ackC <- ack
						}
					}
					startGSN += int64(diff)
				}
			}
			// replace previous committed cut
			s.prevCommittedCut = entry.CommittedCut
		}
		if entry.FinalizeShards != nil { //nolint
			// TODO
		}
	}
}

func (s *DataServer) WaitForAck(cid, csn int32) *datapb.Ack {
	id := int64(cid)<<32 + int64(csn)
	ackC := make(chan *datapb.Ack, 4096)
	s.waitMu.Lock()
	s.wait[id] = ackC
	s.waitMu.Unlock()
	ack := <-ackC
	s.waitMu.Lock()
	delete(s.wait, id)
	s.waitMu.Unlock()
	return ack
}
