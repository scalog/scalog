package order

import (
	log "github.com/scalog/scalogger/logger"

	"github.com/spf13/viper"
)

func Start() {
	numReplica := viper.GetInt("order-replication-factor")
	batchingInterval := viper.GetInt("order-batching-interval")
	log.Printf("%v: %v", "order-replication-factor", numReplica)
	log.Printf("%v: %v", "order-batching-interval", batchingInterval)
}
