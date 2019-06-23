// nolint
package main

import (
	"context"

	"github.com/scalog/scalog/order/orderpb"

	"google.golang.org/grpc"
)

func genLCS(shardID, replicaID int32, cut0, cut1 int64) *orderpb.LocalCuts {
	lc := make([]*orderpb.LocalCut, 1)
	lc[0] = &orderpb.LocalCut{}
	lc[0].ShardID = shardID
	lc[0].LocalReplicaID = replicaID
	cut := make([]int64, 2)
	cut[0] = cut0
	cut[1] = cut1
	lc[0].Cut = cut
	lcs := &orderpb.LocalCuts{Cuts: lc}
	return lcs
}

func main() {
	opts := []grpc.DialOption{grpc.WithInsecure()}
	conn, err := grpc.Dial("127.0.0.1:26733", opts...)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	orderClient := orderpb.NewOrderClient(conn)
	reportClient, err := orderClient.Report(context.Background())
	if err != nil {
		panic(err)
	}

	for i := int64(0); i < 3; i++ {
		lcs := genLCS(0, 0, 18+i, 10+i)
		reportClient.Send(lcs)
		lcs = genLCS(0, 1, 11+i, 17+i)
		reportClient.Send(lcs)
		lcs = genLCS(1, 0, 100+i, 1024+i)
		reportClient.Send(lcs)
		lcs = genLCS(1, 1, 98+i, 2048+i)
		reportClient.Send(lcs)

		cut, err := reportClient.Recv()
		if err != nil {
			panic(err)
		}
		println(cut.String())
	}
}
