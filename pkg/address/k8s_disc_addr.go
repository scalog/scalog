package address

import (
	"github.com/scalog/scalog/pkg/constant"
)

type K8sDiscAddr struct {
	addr string
}

func NewK8sDiscAddr(port uint16) *K8sDiscAddr {
	return &K8sDiscAddr{constant.K8sDiscLeaderAddr(port)}
}

func (s *K8sDiscAddr) UpdatePort(port uint16) {
	s.addr = constant.K8sDiscLeaderAddr(port)
}

func (s *K8sDiscAddr) Get() string {
	return s.addr
}
