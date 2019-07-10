package address

import (
	"fmt"
)

type LocalDataAddr struct {
	numReplica int32
	basePort   uint16
}

func NewLocalDataAddr(numReplica int32, basePort uint16) *LocalDataAddr {
	return &LocalDataAddr{numReplica, basePort}
}

func (s *LocalDataAddr) UpdateBasePort(basePort uint16) {
	s.basePort = basePort
}

func (s *LocalDataAddr) Get(sid, rid int32) string {
	port := s.basePort + uint16(sid*s.numReplica+rid)
	return fmt.Sprintf("127.0.0.1:%v", port)
}
