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

func (storage *Storage) Read(globalSequenceNumber int64) (string, int32, error) {
	record := ""
	localReplicaID := int32(0)
	return record, localReplicaID, nil
}

func (storage *Storage) ReadLocal(localReplicaID int32, localSequenceNumber int64) (string, error) {
	record := ""
	return record, nil
}
