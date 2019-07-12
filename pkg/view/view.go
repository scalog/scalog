package view

import (
	"fmt"
	"sync"

	"github.com/scalog/scalog/discovery/discpb"
)

type View struct {
	ViewID          int32
	Shards          map[int32]bool
	LiveShards      []int32
	FinalizedShards []int32
	mu              sync.RWMutex
}

func NewView() *View {
	return &View{
		ViewID:          -1,
		Shards:          make(map[int32]bool),
		LiveShards:      make([]int32, 0),
		FinalizedShards: make([]int32, 0),
	}

}

func (v *View) Get(sid int32) (bool, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if s, ok := v.Shards[sid]; ok {
		return s, nil
	}
	return false, fmt.Errorf("shard %v doesn't exist", sid)
}

func (v *View) Update(view *discpb.View) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.ViewID = view.ViewID
	v.Shards = make(map[int32]bool)
	v.LiveShards = view.LiveShards
	v.FinalizedShards = view.FinalizedShards
	for _, s := range view.LiveShards {
		v.Shards[s] = true
	}
	for _, s := range view.FinalizedShards {
		v.Shards[s] = false
	}
	return nil
}

func (v *View) Finalize(shards ...int32) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	// check to make sure shard exist
	for _, s := range shards {
		if _, ok := v.Shards[s]; !ok {
			return fmt.Errorf("Shard %v doesn't exist", s)
		}
	}
	// update shard status
	for _, s := range shards {
		if v.Shards[s] {
			v.Shards[s] = false
			v.FinalizedShards = append(v.FinalizedShards, s)
		}
	}
	// reconstruct live shard list
	n := len(v.LiveShards)
	v.LiveShards = make([]int32, n)
	i := 0
	for s, ss := range v.Shards {
		if ss {
			v.LiveShards[i] = s
			i++
		}
	}
	return nil
}

func (v *View) Add(shards ...int32) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	// check to make sure shard doesn't exist
	for _, s := range shards {
		if _, ok := v.Shards[s]; ok {
			return fmt.Errorf("Shard %v exists", s)
		}
	}
	// update shard status
	for _, s := range shards {
		if !v.Shards[s] {
			v.Shards[s] = true
			v.LiveShards = append(v.LiveShards, s)
		}
	}
	return nil
}
