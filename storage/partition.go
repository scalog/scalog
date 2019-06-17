package storage

type Partition struct {
	nextLSN       int64
	segments      []*Segment
	activeSegment *Segment
	activeBaseLSN int64
	segLen        int32
}

func NewPartition() (*Partition, error) {
	var err error
	p := &Partition{nextLSN: 0, activeBaseLSN: 0}
	p.segments = make([]*Segment, 0)
	p.activeSegment, err = NewSegment(p.activeBaseLSN)
	if err != nil {
		return nil, err
	}
	p.segLen = 1000 // TODO: make it configurable
	return p, nil
}

func (p *Partition) Write(record string) (int64, error) {
	lsn := p.nextLSN
	p.nextLSN++
	ssn, err := p.activeSegment.Write(record)
	if ssn >= p.segLen {
		err := p.NewSegment()
		if err != nil {
			return 0, err
		}
	}
	return lsn, err
}

func (p *Partition) NewSegment() error {
	var err error
	p.activeBaseLSN += int64(p.segLen)
	p.segments = append(p.segments, p.activeSegment)
	p.activeSegment, err = NewSegment(p.activeBaseLSN)
	return err
}

func (p *Partition) Assign(lsn, gsn int64) error {
	return nil
}
