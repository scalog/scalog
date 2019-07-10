package address

import (
	"github.com/scalog/scalog/pkg/constant"
)

type K8sDataAddr struct {
	port uint16
}

func NewK8sDataAddr(port uint16) *K8sDataAddr {
	return &K8sDataAddr{port}
}

func (s *K8sDataAddr) UpdatePort(port uint16) {
	s.port = port
}

func (s *K8sDataAddr) Get(sid, rid int32) string {
	return constant.K8sDataServerAddr(sid, rid, s.port)
}
