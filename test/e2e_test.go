package test

import (
	"os"
	"testing"
	"time"

	"github.com/scalog/scalog/client"
	"github.com/scalog/scalog/data"
	disc "github.com/scalog/scalog/discovery"
	"github.com/scalog/scalog/order"
	"github.com/scalog/scalog/pkg/address"

	"github.com/spf13/viper"
)

func TestEnd2End(t *testing.T) {
	// clean up old files
	err := os.RemoveAll("log")
	if err != nil {
		t.Errorf("%v", err)
	}
	// read configuration file
	viper.SetConfigFile("../.scalog.yaml")
	viper.AutomaticEnv()
	err = viper.ReadInConfig()
	if err != nil {
		t.Errorf("read config file error: %v", err)
	}
	// start servers
	for i := 0; i < 3; i++ { // number of raft replicas
		go order.StartOrder(int32(i))
	}
	for i := 0; i < 2; i++ { // number of shards
		for j := 0; j < 2; j++ { // number of replicas in each shard
			go data.StartData(int32(i), int32(j))
		}
	}
	go disc.Start()
	// start a client and run the test
	time.Sleep(2 * time.Second)
	numReplica := int32(viper.GetInt("data-replication-factor"))
	discPort := uint16(viper.GetInt("disc-port"))
	discAddr := address.NewLocalDiscAddr(discPort)
	dataPort := uint16(viper.GetInt("data-port"))
	dataAddr := address.NewLocalDataAddr(numReplica, dataPort)
	cli, err := client.NewClient(dataAddr, discAddr, numReplica)
	if err != nil {
		t.Errorf("create client failure: %v", err)
	}
	time.Sleep(time.Second)
	record := "hello"
	gsn, sid, err := cli.AppendOne(record)
	if err != nil {
		t.Errorf("write record failure: %v", err)
	}
	if gsn < 0 {
		t.Errorf("error global sequence number (should be non-negative): %v", gsn)
	}
	if sid < 0 {
		t.Errorf("error shard (should be non-negative): %v", sid)
	}
	rid := int32(0)
	rec, err := cli.Read(gsn, sid, rid)
	if err != nil {
		t.Errorf("read record failure: %v", err)
	}
	if rec != record {
		t.Errorf("read different from write: %v vs %v", rec, record)
	}
	// clean up
	err = os.RemoveAll("log")
	if err != nil {
		t.Errorf("%v", err)
	}
}
