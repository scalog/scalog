package storage

type Partition struct {
	path          string
	nextLSN       int64
	segments      []*Segment
	activeSegment *Segment
	activeBaseLSN int64
	segLen        int32
}

func NewPartition(path string, segLen int32) (*Partition, error) {
	var err error
	p := &Partition{path: path, nextLSN: 0, activeBaseLSN: 0}
	p.segments = make([]*Segment, 0)
	p.activeSegment, err = NewSegment(path, p.activeBaseLSN)
	if err != nil {
		return nil, err
	}
	p.segLen = segLen
	return p, nil
}

func (p *Partition) Write(record string) (int64, error) {
	lsn := p.nextLSN
	p.nextLSN++
	ssn, err := p.activeSegment.Write(record)
	if ssn >= p.segLen {
		err := p.CreateSegment()
		if err != nil {
			return 0, err
		}
	}
	return lsn, err
}

func (p *Partition) Read(gsn int64) (string, error) {
	return p.ReadGSN(gsn)
}

func (p *Partition) ReadGSN(gsn int64) (string, error) {
	f := func(s *Segment) int64 {
		return s.BaseGSN
	}
	s := binarySearch(p.segments, f, gsn)
	return s.ReadLSN(gsn)
}

func (p *Partition) ReadLSN(lsn int64) (string, error) {
	f := func(s *Segment) int64 {
		return s.BaseLSN
	}
	s := binarySearch(p.segments, f, lsn)
	return s.ReadLSN(lsn)
}

func binarySearch(segs []*Segment, get func(*Segment) int64, target int64) *Segment {
	// Currently the function is scanning the list.
	// TODO: implement binary search.
	for i, s := range segs {
		if get(s) > target {
			return segs[i-1]
		}
	}
	return nil
}

func (p *Partition) CreateSegment() error {
	var err error
	p.activeBaseLSN += int64(p.segLen)
	p.segments = append(p.segments, p.activeSegment)
	p.activeSegment, err = NewSegment(p.path, p.activeBaseLSN)
	return err
}

func (p *Partition) Assign(lsn int64, length int32, gsn int64) error {
	return p.activeSegment.Assign(int32(lsn-p.activeBaseLSN), length, gsn)
}
