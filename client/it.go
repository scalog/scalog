package client

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/scalog/scalog/pkg/address"

	"github.com/spf13/viper"
)

type It struct {
	client *Client
}

func NewIt() (*It, error) {
	numReplica := int32(viper.GetInt("data-replication-factor"))
	discPort := uint16(viper.GetInt("disc-port"))
	discAddr := address.NewLocalDiscAddr(discPort)
	dataPort := uint16(viper.GetInt("data-port"))
	dataAddr := address.NewLocalDataAddr(numReplica, dataPort)
	client, err := NewClient(dataAddr, discAddr, numReplica)
	if err != nil {
		return nil, err
	}
	it := &It{client}
	return it, nil
}

func NewK8sIt() (*It, error) {
	numReplica := int32(viper.GetInt("data-replication-factor"))
	discPort := uint16(viper.GetInt("disc-port"))
	discAddr := address.NewK8sDiscAddr(discPort)
	dataPort := uint16(viper.GetInt("data-port"))
	dataAddr := address.NewK8sDataAddr(dataPort)
	client, err := NewClient(dataAddr, discAddr, numReplica)
	if err != nil {
		return nil, err
	}
	it := &It{client}
	return it, nil
}

func (it *It) Test() {
	record := "hello"
	gsn, sid, err := it.client.AppendOne(record)
	if err != nil {
		fmt.Fprintf(os.Stderr, "write record failure: %v", err)
	}
	if gsn < 0 {
		fmt.Fprintf(os.Stderr, "error global sequence number (should be non-negative): %v", gsn)
	}
	if sid < 0 {
		fmt.Fprintf(os.Stderr, "error shard (should be non-negative): %v", sid)
	}
	rid := int32(0)
	rec, err := it.client.Read(gsn, sid, rid)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read record failure: %v", err)
	}
	if rec != record {
		fmt.Fprintf(os.Stderr, "read different from write: %v vs %v", rec, record)
	}
	fmt.Fprintf(os.Stderr, "client test completed")
}

func (it *It) Start() {
	regex := regexp.MustCompile(" +")
	reader := bufio.NewReader(os.Stdin)
	for {
		cmdString, err := reader.ReadString('\n')
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			continue
		}
		cmdString = strings.TrimSuffix(cmdString, "\n")
		cmdString = strings.Trim(cmdString, " ")
		if cmdString == "" {
			continue
		}
		cmd := regex.Split(cmdString, -1)
		if cmd[0] == "quit" || cmd[0] == "exit" {
			break
		} else if cmd[0] == "append" {
			if len(cmd) < 2 {
				fmt.Fprintln(os.Stderr, "Command error: missing required argument [record]")
				continue
			}
			record := strings.Join(cmd[1:], " ")
			gsn, shard, err := it.client.AppendOne(record)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				continue
			}
			fmt.Fprintf(os.Stderr, "Append result: { Gsn: %d, Shard: %d, Record: %s }\n", gsn, shard, record)
		} else if cmd[0] == "read" {
			if len(cmd) != 3 {
				fmt.Fprintln(os.Stderr, "Command error: missing required argument [record]")
				continue
			}
			gsn, err := strconv.Atoi(cmd[1])
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				continue
			}
			sid, err := strconv.Atoi(cmd[2])
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				continue
			}
			rid := int32(0)
			record, err := it.client.Read(int64(gsn), int32(sid), rid)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				continue
			}
			fmt.Fprintf(os.Stderr, "%v\n", record)
		} else if cmd[0] == "help" {
			fmt.Fprintln(os.Stderr, `Supported commands:
			append [Record]
			read [GSN] [ShardID]
			exit`)
		} else {
			fmt.Fprintln(os.Stderr, "Command error: invalid command")
		}
	}
}
