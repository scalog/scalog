package constant

import (
	"fmt"
)

func K8sOrderLeaderDomain() string {
	return "dns:///leader.order.scalog"
}

func K8sOrderLeaderAddr(port uint16) string {
	return fmt.Sprintf("%v:%v", K8sOrderLeaderDomain(), port)
}

func K8sDataServerDomain(sid, rid int32) string {
	return fmt.Sprintf("dns:///%v-%v.data.scalog", sid, rid)
}

func K8sDataServerAddr(sid, rid int32, port uint16) string {
	return fmt.Sprintf("%v:%v", K8sDataServerDomain(sid, rid), port)
}

func K8sDiscLeaderDomain() string {
	return "dns:///service.disc.scalog"
}

func K8sDiscLeaderAddr(port uint16) string {
	return fmt.Sprintf("%v:%v", K8sDiscLeaderDomain(), port)
}
