package order

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scalog/scalog/order/orderpb"

	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft/raftpb"
)

type OrderServer struct {
	index            int32
	numReplica       int32
	dataNumReplica   int32
	clientID         int32
	batchingInterval time.Duration
	isLeader         bool
	startGSN         int64
	viewID           int32          // use sync/atomic to access viewID
	shards           map[int32]bool // true for live shards, false for finalized ones
	forwardC         chan *orderpb.LocalCuts
	proposeC         chan *orderpb.CommittedEntry
	commitC          chan *orderpb.CommittedEntry
	finalizeC        chan *orderpb.FinalizeEntry
	subC             map[int32]chan *orderpb.CommittedEntry
	subCMu           sync.RWMutex
	prevCut          map[int32]int64
	snapMu           sync.Mutex

	rnConfChangeC      chan raftpb.ConfChange
	rnProposeC         chan string
	rnCommitC          <-chan *string
	rnErrorC           <-chan error
	rnSnapshotterReady <-chan *snap.Snapshotter
}

func NewOrderServer(index, numReplica, dataNumReplica int32, batchingInterval time.Duration, peerList []string) *OrderServer {
	s := &OrderServer{
		index:            index,
		numReplica:       numReplica,
		dataNumReplica:   dataNumReplica,
		viewID:           0,
		isLeader:         index == 0,
		batchingInterval: batchingInterval,
	}
	s.shards = make(map[int32]bool)
	s.forwardC = make(chan *orderpb.LocalCuts, 4096)
	s.proposeC = make(chan *orderpb.CommittedEntry, 4096)
	s.commitC = make(chan *orderpb.CommittedEntry, 4096)
	s.finalizeC = make(chan *orderpb.FinalizeEntry, 4096)
	s.subC = make(map[int32]chan *orderpb.CommittedEntry)

	s.rnConfChangeC = make(chan raftpb.ConfChange)
	s.rnProposeC = make(chan string)
	commitC, errorC, snapshotterReady := newRaftNode(
		int(index)+1, // raftNode is 1-indexed
		peerList,
		false, // not to join an existing cluster
		s.getSnapshot,
		s.rnProposeC,
		s.rnConfChangeC,
	)
	s.rnCommitC = commitC
	s.rnErrorC = errorC
	s.rnSnapshotterReady = snapshotterReady
	return s
}

func (s *OrderServer) Start() {
	go s.processReport()
	go s.runReplication()
	go s.processCommit()
}

// runReplication runs Raft to replicate proposed messages and receive
// committed messages.
func (s *OrderServer) runReplication() {
	for e := range s.proposeC {
		s.commitC <- e
	}
}

func (s *OrderServer) computeCutDiff(pcut, ccut map[int32]int64) int64 {
	sum := int64(0)
	if pcut == nil {
		for _, v := range ccut {
			sum += v
		}
	} else {
		for k, v := range ccut {
			if vv, ok := pcut[k]; ok {
				sum += v - vv
			} else {
				sum += v
			}
		}
	}
	return sum

}

func (s *OrderServer) computeCommittedCut(lcs map[int32]*orderpb.LocalCut) map[int32]int64 {
	incrViewID := false
	// add new live shards
	for shard := range lcs {
		if _, ok := s.shards[shard]; !ok {
			incrViewID = true
			s.shards[shard] = true
		}
	}
	ccut := make(map[int32]int64)
	for shard, status := range s.shards {
		// check if the shard is finialized
		if !status {
			incrViewID = true
			// clean finalized shards from lcs
			delete(lcs, shard)
			continue
		}
		localReplicaID := shard % s.dataNumReplica
		begin := shard - localReplicaID
		min := int64(math.MaxInt64)
		for i := int32(0); i < s.dataNumReplica; i++ {
			if min > lcs[begin+i].Cut[localReplicaID] {
				min = lcs[begin+i].Cut[localReplicaID]
			}
		}
		ccut[shard] = min
	}
	if incrViewID {
		atomic.AddInt32(&s.viewID, 1)
	}
	return ccut
}

// proposeCommit broadcasts entries in commitC to all subCs.
func (s *OrderServer) processReport() {
	lcs := make(map[int32]*orderpb.LocalCut) // all local cuts
	ticker := time.NewTicker(s.batchingInterval)
	for {
		select {
		case e := <-s.forwardC:
			if s.isLeader { // store local cuts
				for _, lc := range e.Cuts {
					id := lc.ShardID*s.dataNumReplica + lc.LocalReplicaID
					valid := true
					// check if the received cut is up-to-date
					if _, ok := lcs[id]; ok {
						for i := int32(0); i < s.dataNumReplica; i++ {
							if lc.Cut[i] < lcs[id].Cut[i] {
								valid = false
							}
						}
					}
					if valid {
						lcs[id] = lc
					}
				}
			} else {
				// TODO: forward to the leader
			}
		case <-ticker.C:
			// TODO: check to make sure the key in lcs exist
			if s.isLeader { // compute committedCut
				ccut := s.computeCommittedCut(lcs)
				vid := atomic.LoadInt32(&s.viewID)
				ce := &orderpb.CommittedEntry{Seq: 0, ViewID: vid, CommittedCut: &orderpb.CommittedCut{StartGSN: s.startGSN, Cut: ccut}, FinalizeShards: nil}
				s.proposeC <- ce
				s.startGSN += s.computeCutDiff(s.prevCut, ccut)
				s.prevCut = ccut
			}
		}
	}
}

// proposeCommit broadcasts entries in commitC to all subCs.
func (s *OrderServer) processCommit() {
	for e := range s.commitC {
		s.subCMu.RLock()
		for _, c := range s.subC {
			c <- e
		}
		s.subCMu.RUnlock()
	}
}

func (server *OrderServer) getSnapshot() ([]byte, error) {
	b := make([]byte, 0)
	return b, nil
}

func (server *OrderServer) attemptRecoverFromSnapshot() {
}
