package address

import (
	"github.com/scalog/scalog/pkg/constant"
)

type K8sOrderAddr struct {
	addr string
}

func NewK8sOrderAddr(port uint16) *K8sOrderAddr {
	return &K8sOrderAddr{constant.K8sOrderLeaderAddr(port)}
}

func (s *K8sOrderAddr) UpdatePort(port uint16) {
	s.addr = constant.K8sOrderLeaderAddr(port)
}

func (s *K8sOrderAddr) Get() string {
	return s.addr
}
