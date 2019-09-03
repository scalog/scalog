package client

import (
	log "github.com/scalog/scalog/logger"
)

func Start() {
	it, err := NewIt()
	if err != nil {
		log.Fatalf("%v", err)
		return
	}
	it.Start()
}
