package address

type LocalDiscAddr struct {
	addr string
}

func NewLocalDiscAddr(addr string) *LocalDiscAddr {
	return &LocalDiscAddr{addr}
}

func (s *LocalDiscAddr) UpdateAddr(addr string) {
	s.addr = addr
}

func (s *LocalDiscAddr) Get() string {
	return s.addr
}
