package storage

type Storage struct {
	path          string
	numPartitions int32
	partitionID   int32
	partitions    []*Partition
}

func NewStorage(path string, partitionID, numPartitions int32) (*Storage, error) {
	var err error
	s := &Storage{
		path:          path,
		partitionID:   partitionID,
		numPartitions: numPartitions,
	}
	s.partitions = make([]*Partition, numPartitions)
	for i := int32(0); i < numPartitions; i++ {
		s.partitions[i], err = NewPartition()
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Storage) Write(record string) (int64, error) {
	lsn, err := s.WritePartition(s.partitionID, record)
	return lsn, err
}

func (s *Storage) WritePartition(id int32, record string) (int64, error) {
	lsn, err := s.partitions[id].Write(record)
	return lsn, err
}

func (s *Storage) Assign(partitionID int32, lsn, gsn int64) error {
	return s.partitions[partitionID].Assign(lsn, gsn)
}

func (s *Storage) Read(gsn int64) (string, error) {
	record := ""
	return record, nil
}

func (s *Storage) ReadPartition(id int32, lsn int64) (string, error) {
	record := ""
	return record, nil
}
