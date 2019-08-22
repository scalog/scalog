package client

import (
	log "github.com/scalog/scalog/logger"
)

func StartK8s() {
	it, err := NewK8sIt()
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	it.Test()
	it.Start()
}
