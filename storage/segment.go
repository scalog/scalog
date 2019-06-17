package storage

type Segment struct {
	base    int64
	nextSSN int32
}

func NewSegment(base int64) (*Segment, error) {
	s := &Segment{base: base}
	return s, nil
}

func (s *Segment) Write(record string) (int32, error) {
	ssn := s.nextSSN
	s.nextSSN++
	return ssn, nil
}
