package data

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/scalog/scalog/data/datapb"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/order/orderpb"
	"github.com/scalog/scalog/storage"

	"google.golang.org/grpc"
)

type DataServer struct {
	// data server configurations
	shardID          int32
	replicaID        int32
	numReplica       int32
	batchingInterval time.Duration
	// server state
	clientID         int32 // incremental counter to distinguish clients
	viewID           int32
	localCut         []int64
	localCutMu       sync.Mutex
	prevCommittedCut *orderpb.CommittedCut
	// ordering layer information
	orderAddr   string
	orderConn   *grpc.ClientConn
	orderClient *orderpb.Order_ReportClient
	orderMu     sync.RWMutex
	// peer information
	peers       []string
	peerConns   []*grpc.ClientConn
	peerClients []*datapb.Data_ReplicateClient
	peerDoneC   []chan interface{}
	peerMu      sync.Mutex
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
}

func NewDataServer(replicaID, shardID, numReplica int32, batchingInterval time.Duration, peers string, orderAddr string) *DataServer {
	server := &DataServer{
		replicaID:        replicaID,
		shardID:          shardID,
		clientID:         0,
		viewID:           0,
		batchingInterval: batchingInterval,
	}
	server.localCut = make([]int64, numReplica)
	for i := 0; i < int(numReplica); i++ {
		server.localCut[i] = 0
	}
	server.committedEntryC = make(chan *orderpb.CommittedEntry, 4096)
	server.ackSendC = make(map[int32]chan *datapb.Ack)
	server.subC = make(map[int32]chan *datapb.Record)
	server.ackC = make(chan *datapb.Ack, 4096)
	server.appendC = make(chan *datapb.Record, 4096)
	server.replicateC = make(chan *datapb.Record, 4096)
	server.replicateSendC = make([]chan *datapb.Record, numReplica)
	server.peerDoneC = make([]chan interface{}, numReplica)
	path := fmt.Sprintf("storage-%v-%v", shardID, replicaID) // TODO configure path
	segLen := int32(1000)                                    // TODO configurable segment length
	storage, err := storage.NewStorage(path, replicaID, numReplica, segLen)
	if err != nil {
		log.Fatalf("Create storage failed: %v", err)
	}
	server.storage = storage
	for i := int32(0); i < numReplica; i++ {
		server.replicateSendC[i] = make(chan *datapb.Record, 4096)
	}
	server.UpdateOrderAddr(orderAddr)
	server.UpdatePeers(peers)
	return server
}

func (server *DataServer) UpdateOrderAddr(addr string) error {
	server.orderMu.Lock()
	defer server.orderMu.Unlock()
	if server.orderConn != nil {
		server.orderConn.Close()
	}
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

// UpdatePeers updates the peer list of the shard. It should be called only at
// the initialization phase of the server.
// TODO make the list updatable when running
func (server *DataServer) UpdatePeers(peers string) error {
	// check if the number of peers matches that in the configuration
	ps := strings.Split(peers, ",")
	if int32(len(ps)) != server.numReplica {
		return fmt.Errorf("the number of peers in peer list doesn't match the number of replicas: %v vs %v", len(ps), server.numReplica)
	}
	// create connections with peers
	server.peers = ps
	server.peerConns = make([]*grpc.ClientConn, server.numReplica)
	server.peerClients = make([]*datapb.Data_ReplicateClient, server.numReplica)
	for i := int32(0); i < server.numReplica; i++ {
		err := server.connectToPeers(i)
		if err != nil {
			log.Errorf("%v", err)
			continue
		}
		done := make(chan interface{})
		server.peerMu.Lock()
		sendC := server.replicateSendC[i]
		client := server.peerClients[i]
		server.peerDoneC[i] = done
		server.peerMu.Unlock()
		go server.replicateRecords(done, sendC, client)
	}
	return nil
}

func (server *DataServer) Start() {
	go server.processAppend()
	go server.processReplicate()
	go server.processAck()
	go server.processCommittedEntry()
	go server.reportLocalCut()
	go server.receiveCommittedCut()
}

func (server *DataServer) connectToPeers(peer int32) error {
	server.peerMu.Lock()
	defer server.peerMu.Unlock()
	// do not connect to the node itself
	if peer == server.replicaID {
		return nil
	}
	// close existing network connections if they exist
	if server.peerConns[peer] != nil {
		server.peerConns[peer].Close()
		server.peerConns[peer] = nil
	}
	if server.peerDoneC[peer] != nil {
		close(server.peerDoneC[peer])
		server.peerDoneC = nil
	}
	// build connections to peers
	opts := []grpc.DialOption{grpc.WithInsecure()}
	conn, err := grpc.Dial(server.peers[peer], opts...)
	if err != nil {
		return fmt.Errorf("Dial peer %v failed: %v", server.peers[peer], err)
	}
	server.peerConns[peer] = conn
	dataClient := datapb.NewDataClient(conn)
	replicateSendClient, err := dataClient.Replicate(context.Background())
	if err != nil {
		return fmt.Errorf("Create replicate client to %v failed: %v", server.peers[peer], err)
	}
	if server.peerConns[peer] != nil {
		server.peerConns[peer].Close()
	}
	server.peerClients[peer] = &replicateSendClient
	return nil
}

func (server *DataServer) replicateRecords(done <-chan interface{}, ch chan *datapb.Record, client *datapb.Data_ReplicateClient) {
	for {
		select {
		case record := <-ch:
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
func (server *DataServer) processAppend() {
	for record := range server.appendC {
		record.LocalReplicaID = server.replicaID
		server.replicateC <- record
		for i, c := range server.replicateSendC {
			if int32(i) != server.replicaID {
				c <- record
			}
		}
	}
}

// processReplicate writes records to local storage
func (server *DataServer) processReplicate() {
	for record := range server.replicateC {
		lsn, err := server.storage.WriteToPartition(record.LocalReplicaID, record.Record)
		if err != nil {
			log.Fatalf("Write to storage failed: %v", err)
		}
		server.localCutMu.Lock()
		server.localCut[record.LocalReplicaID] = lsn
		server.localCutMu.Unlock()
	}
}

func (server *DataServer) processAck() {
	for ack := range server.ackC {
		// send to ack channel
		server.ackSendCMu.RLock()
		c := server.ackSendC[ack.ClientID]
		server.ackSendCMu.RUnlock()
		c <- ack
		// send individual ack message if requested to block
		id := int64(ack.ClientID)<<32 + int64(ack.ClientSN)
		server.waitMu.RLock()
		c, ok := server.wait[id]
		server.waitMu.RUnlock()
		if ok {
			c <- ack
			break
		}
	}
}

func (server *DataServer) reportLocalCut() {
	tick := time.NewTicker(time.Millisecond)
	for {
		select {
		case <-tick.C:
			lcs := &orderpb.LocalCuts{}
			lcs.Cuts = make([]*orderpb.LocalCut, 1)
			lcs.Cuts[0].ShardID = server.shardID
			lcs.Cuts[0].LocalReplicaID = server.replicaID
			server.localCutMu.Lock()
			copy(lcs.Cuts[0].Cut, server.localCut)
			server.localCutMu.Unlock()
			(*server.orderClient).Send(lcs)
		}
	}
}

func (server *DataServer) receiveCommittedCut() {
	for {
		e, err := (*server.orderClient).Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Errorf("Receive from ordering layer error: %v", err)
		}
		server.committedEntryC <- e
	}
}

func (server *DataServer) processCommittedEntry() {
	for entry := range server.committedEntryC {
		if entry.CommittedCut != nil {
			startReplicaID := server.shardID * server.numReplica
			startGSN := entry.CommittedCut.StartGSN
			// compute startGSN using the number of records stored
			// in shards with smaller ids
			for rid, lsn := range entry.CommittedCut.Cut {
				if rid < startReplicaID {
					diff := lsn
					if l, ok := server.prevCommittedCut.Cut[rid]; ok {
						diff = lsn - l
					}
					if diff > 0 {
						startGSN += int64(diff)
					}
				}
			}
			// assign gsn to records in my shard
			for i := int32(0); i < server.numReplica; i++ {
				rid := startReplicaID + i
				lsn := entry.CommittedCut.Cut[rid]
				diff := int32(lsn)
				start := int64(0)
				if l, ok := server.prevCommittedCut.Cut[rid]; ok {
					start = l
					diff = int32(lsn - l)
				}
				if diff > 0 {
					server.storage.Assign(i+startReplicaID, start, diff, startGSN)
					startGSN += int64(diff)
				}
			}
			// replace previous committed cut
			server.prevCommittedCut = entry.CommittedCut
		}
		if entry.FinalizeShards != nil {
		}
	}
}

func (server *DataServer) WaitForAck(cid, csn int32) *datapb.Ack {
	id := int64(cid)<<32 + int64(csn)
	ackC := make(chan *datapb.Ack, 4096)
	server.waitMu.Lock()
	server.wait[id] = ackC
	server.waitMu.Unlock()
	select {
	case ack := <-ackC:
		server.waitMu.Lock()
		delete(server.wait, id)
		server.waitMu.Unlock()
		return ack
	}
}
