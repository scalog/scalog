package data

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/scalog/scalogger/data/datapb"
	log "github.com/scalog/scalogger/logger"

	"google.golang.org/grpc"
)

type DataServer struct {
	shardID          int32
	localReplicaID   int32
	globalReplicaID  int32
	clientID         int32
	numReplica       int32
	peers            []string
	peerConns        []*grpc.ClientConn
	peerClients      []*datapb.Data_ReplicateClient
	batchingInterval time.Duration
	appendC          chan *datapb.Record
	replicateC       []chan *datapb.Record
	ackC             map[int32]chan *datapb.Ack
	ackCMu           sync.RWMutex
}

func NewDataServer(replicaID, shardID, numReplica int32, batchingInterval time.Duration, peers string) *DataServer {
	server := &DataServer{
		localReplicaID:   replicaID,
		shardID:          shardID,
		globalReplicaID:  shardID*numReplica + replicaID,
		clientID:         0,
		batchingInterval: batchingInterval,
	}
	server.ackC = make(map[int32]chan *datapb.Ack)
	server.appendC = make(chan *datapb.Record)
	server.replicateC = make([]chan *datapb.Record, numReplica)
	for i := int32(0); i < numReplica; i++ {
		server.replicateC[i] = make(chan *datapb.Record)
	}
	server.UpdatePeers(peers)
	return server
}

// UpdatePeers updates the peer list of the shard. It should be called only at
// the initialization phase of the server.
// TODO make the list updatable when running
func (server *DataServer) UpdatePeers(peers string) {
	// check if the number of peers matches that in the configuration
	ps := strings.Split(peers, ",")
	if int32(len(ps)) != server.numReplica {
		log.Errorf("the number of peers in peer list doesn't match the number of replicas: %v vs %v", len(ps), server.numReplica)
		return
	}
	// close existing network connections if they exist
	if server.peerConns != nil {
		for _, c := range server.peerConns {
			c.Close()
		}
	}
	// create connections with peers
	server.peers = ps
	server.peerConns = make([]*grpc.ClientConn, server.numReplica)
	server.peerClients = make([]*datapb.Data_ReplicateClient, server.numReplica)
	opts := []grpc.DialOption{grpc.WithInsecure()}
	for i := int32(0); i < server.numReplica; i++ {
		if i == server.localReplicaID {
			continue
		}
		conn, err := grpc.Dial(server.peers[i], opts...)
		if err != nil {
			log.Fatalf("Dial peer %v failed: %v", server.peers[i], err)
		}
		server.peerConns[i] = conn
		dataClient := datapb.NewDataClient(conn)
		replicateClient, err := dataClient.Replicate(context.Background())
		if err != nil {
			log.Fatalf("Create replicate client to %v failed: %v", server.peers[i], err)
		}
		server.peerClients[i] = &replicateClient
		go server.replicateRecords(i)
	}
}

func (server *DataServer) Start() {
	go server.processAppend()
}

func (server *DataServer) replicateRecords(i int32) {
	ch := server.replicateC[i]
	client := *server.peerClients[i]
	for {
		select {
		case record := <-ch:
			err := client.Send(record)
			if err != nil {
				log.Errorf("Send record to peer %v error: %v", i, err)
			}
		}
	}
}

func (server *DataServer) processAppend() {
	for {
		select {
		case record := <-server.appendC:
			for _, c := range server.replicateC {
				c <- record
			}
		}
	}
}
