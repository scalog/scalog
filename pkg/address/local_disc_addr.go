package address

import (
	"fmt"
)

type LocalDiscAddr struct {
	port uint16
}

func NewLocalDiscAddr(port uint16) *LocalDiscAddr {
	return &LocalDiscAddr{port}
}

func (s *LocalDiscAddr) UpdateAddr(port uint16) {
	s.port = port
}

func (s *LocalDiscAddr) Get() string {
	return fmt.Sprintf("127.0.0.1:%v", s.port)
}
