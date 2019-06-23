package client

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/spf13/viper"
)

type It struct {
	client *Client
}

func NewIt() (*It, error) {
	discAddr := viper.GetString("discovery-addr")
	numReplica := int32(viper.GetInt("data-replication-factor"))
	client, err := NewLocalClient(discAddr, numReplica)
	if err != nil {
		return nil, err
	}
	it := &It{}
	it.client = client
	return it, nil
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
		}
		if cmd[0] == "append" {
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
		} else if cmd[0] == "help" {
			fmt.Fprintln(os.Stderr, "Supported commands:")
			fmt.Fprintln(os.Stderr, "    append [record]")
			fmt.Fprintln(os.Stderr, "    appendToShard [record]")
			fmt.Fprintln(os.Stderr, "    subscribe [gsn]")
			fmt.Fprintln(os.Stderr, "    readRecord [gsn] [shardID]")
			fmt.Fprintln(os.Stderr, "    trim [gsn]")
			fmt.Fprintln(os.Stderr, "    exit")
		} else {
			fmt.Fprintln(os.Stderr, "Command error: invalid command")
		}
	}
}
