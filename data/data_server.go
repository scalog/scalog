package data

import (
	"context"
	"fmt"
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
	shardID          int32
	localReplicaID   int32
	globalReplicaID  int32
	clientID         int32
	numReplica       int32
	viewID           int32
	peers            []string
	peerConns        []*grpc.ClientConn
	peerClients      []*datapb.Data_ReplicateClient
	orderAddr        string
	orderConn        *grpc.ClientConn
	orderClient      *orderpb.Order_ReportClient
	batchingInterval time.Duration
	appendC          chan *datapb.Record
	replicateC       chan *datapb.Record
	replicateSendC   []chan *datapb.Record
	ackC             map[int32]chan *datapb.Ack
	ackCMu           sync.RWMutex
	subC             map[int32]chan *datapb.Record
	subCMu           sync.RWMutex
	orderMu          sync.RWMutex
	storage          *storage.Storage
	peerDoneC        []chan interface{}
}

func NewDataServer(replicaID, shardID, numReplica int32, batchingInterval time.Duration, peers string, orderAddr string) *DataServer {
	server := &DataServer{
		localReplicaID:   replicaID,
		shardID:          shardID,
		globalReplicaID:  shardID*numReplica + replicaID,
		clientID:         0,
		viewID:           0,
		batchingInterval: batchingInterval,
	}
	server.ackC = make(map[int32]chan *datapb.Ack)
	server.subC = make(map[int32]chan *datapb.Record)
	server.appendC = make(chan *datapb.Record)
	server.replicateC = make(chan *datapb.Record)
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
		server.replicateSendC[i] = make(chan *datapb.Record)
	}
	server.UpdateOrderAddr(orderAddr)
	server.UpdatePeers(peers)
	return server
}

func (server *DataServer) UpdateOrderAddr(addr string) error {
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
		go server.replicateRecords(done, server.replicateSendC[i], server.peerClients[i])
		server.peerDoneC[i] = done
	}
	return nil
}

func (server *DataServer) Start() {
	go server.processAppend()
	go server.processReplicate()
}

func (server *DataServer) connectToPeers(peer int32) error {
	if peer == server.localReplicaID {
		return nil // do not connect to the node itself
	}
	// close existing network connections if they exist
	if server.peerConns[peer] != nil {
		for _, c := range server.peerConns {
			c.Close()
		}
	}
	if server.peerDoneC[peer] != nil {
		close(server.peerDoneC[peer])
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
	for {
		select {
		case record := <-server.appendC:
			record.LocalReplicaID = server.localReplicaID
			server.replicateC <- record
			for _, c := range server.replicateSendC {
				c <- record
			}
		}
	}
}

// processReplicate writes records to local storage
func (server *DataServer) processReplicate() {
	for {
		select {
		case record := <-server.replicateC:
			_, err := server.storage.WriteToPartition(record.LocalReplicaID, record.Record)
			if err != nil {
				log.Fatalf("Write to storage failed: %v", err)
			}
		}
	}
}
