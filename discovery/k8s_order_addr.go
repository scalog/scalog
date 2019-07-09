package discovery

import (
	"fmt"
)

const OrderLeaderDomain = "dns:///leader.order.scalog"

type K8sOrderAddr struct {
	addr string
}

func NewK8sOrderAddr(port uint16) *K8sOrderAddr {
	addr := fmt.Sprintf("%v:%v", OrderLeaderDomain, port)
	return &K8sOrderAddr{addr}
}

func (s K8sOrderAddr) Update(port uint16) {
	s.addr = fmt.Sprintf("%v:%v", OrderLeaderDomain, port)
}

func (s K8sOrderAddr) Get() string {
	return s.addr
}
