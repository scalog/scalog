package order_addr

type LocalOrderAddr struct {
	addr string
}

func NewLocalOrderAddr(addr string) *LocalOrderAddr {
	return &LocalOrderAddr{addr}
}

func (s *LocalOrderAddr) UpdateAddr(addr string) {
	s.addr = addr
}

func (s *LocalOrderAddr) Get() string {
	return s.addr
}
