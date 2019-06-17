package data

import (
	"github.com/scalog/scalogger/data/datapb"
)

type Storage struct {
}

func NewStorage() *Storage {
	return &Storage{}
}

func (storage *Storage) Write(record *datapb.Record) error {
	return nil
}

func (storage *Storage) Read(localReplicaID int32, globalSequenceNumber int64) error {
	return nil
}

func (storage *Storage) ReadLocal(localReplicaID int32, localSequenceNumber int64) error {
	return nil
}
