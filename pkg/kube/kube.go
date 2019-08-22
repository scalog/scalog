package kube

import (
	"time"

	log "github.com/scalog/scalog/logger"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

/*
InitKubernetesClient returns a client for interacting with the
kubernetes API server.
*/
func InitKubernetesClient() *kubernetes.Clientset {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Panicf(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Panicf(err.Error())
	}
	return clientset
}

/*
AllPodsAreRunning returns true if and only if [pods] contains
only kubernetes pod objects that are in the "running" state.
*/
func AllPodsAreRunning(pods []v1.Pod) bool {
	for _, pod := range pods {
		if pod.Status.Phase != v1.PodRunning {
			return false
		}
	}
	return true
}

/*
GetShardPods blocks until the specified number of pods have appeared
*/
func GetShardPods(clientset *kubernetes.Clientset, labelSelector string, expectedPodCount int, namespace string) *v1.PodList {
	query := metav1.ListOptions{LabelSelector: labelSelector}

	for {
		pods, err := clientset.CoreV1().Pods(namespace).List(query)
		if err != nil {
			log.Panicf(err.Error())
		}
		if len(pods.Items) == expectedPodCount && AllPodsAreRunning(pods.Items) {
			return pods
		}
		log.Infof("wait for pods to start up")
		time.Sleep(1000 * time.Millisecond)
	}
}
