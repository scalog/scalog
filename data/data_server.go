package data

import (
	"sync"
	"time"

	"github.com/scalog/scalogger/data/datapb"
)

type DataServer struct {
	shardID          int32
	localReplicaID   int32
	globalReplicaID  int32
	clientID         int32
	batchingInterval time.Duration
	appendC          chan *datapb.Record
	ackC             map[int32]chan *datapb.Ack
	ackCMu           sync.RWMutex
}

func NewDataServer(replicaID, shardID, numReplica int32, batchingInterval time.Duration) *DataServer {
	server := &DataServer{
		localReplicaID:   replicaID,
		shardID:          shardID,
		globalReplicaID:  shardID*numReplica + replicaID,
		clientID:         0,
		batchingInterval: batchingInterval,
	}
	server.ackC = make(map[int32]chan *datapb.Ack)
	return server
}
