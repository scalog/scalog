package address

import (
	"fmt"

	"github.com/scalog/scalog/pkg/constant"
	log "github.com/scalog/scalog/logger"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/scalog/scalog/internal/pkg/kube"
	"github.com/spf13/viper"
)

type K8sOrderAddr struct {
	addr string
}

func NewK8sOrderAddr(port uint16) *K8sOrderAddr {
	// FIXME: this is a workaround to find the ordering leader node
	query := metav1.ListOptions{LabelSelector: "app=scalog-order"}
	pods, err := kube.InitKubernetesClient().CoreV1().Pods(viper.GetString("namespace")).List(query)
	if err != nil {
		log.Panicf(err.Error())
	}

	orderLeaderIP := pods.Items[0].Status.PodIP
	orderLeaderUID := pods.Items[0].UID
	for _, pod := range pods.Items {
		log.Printf("Peer ip: " + pod.Status.PodIP + ", uid: " + string(pod.UID))
		if pod.UID < orderLeaderUID {
			orderLeaderUID = pod.UID
			orderLeaderIP = pod.Status.PodIP
		}
	}
	return &K8sOrderAddr{fmt.Sprintf("%v:%v", orderLeaderIP, port)}

	// return &K8sOrderAddr{constant.K8sOrderLeaderAddr(port)}
}

func (s *K8sOrderAddr) UpdatePort(port uint16) {
	s.addr = constant.K8sOrderLeaderAddr(port)
}

func (s *K8sOrderAddr) Get() string {
	return s.addr
}
