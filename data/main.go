package data

import (
	log "github.com/scalog/scalogger/logger"

	"github.com/spf13/viper"
)

func Start() {
	numReplica := viper.GetInt("data-replication-factor")
	batchingInterval := viper.GetString("data-batching-interval")
	log.Printf("%v: %v", "data-replication-factor", numReplica)
	log.Printf("%v: %v", "data-batching-interval", batchingInterval)
}
