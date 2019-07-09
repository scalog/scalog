package discovery

type LocalOrderAddr struct {
	addr string
}

func NewLocalOrderAddr(addr string) *LocalOrderAddr {
	return &LocalOrderAddr{addr}
}

func (s LocalOrderAddr) Update(addr string) {
	s.addr = addr
}

func (s LocalOrderAddr) Get() string {
	return s.addr
}
