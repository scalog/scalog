package client

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scalog/scalog/data/datapb"
	"github.com/scalog/scalog/discovery/discpb"
	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/pkg/address"
	"github.com/scalog/scalog/pkg/view"

	"google.golang.org/grpc"
)

type ShardingPolicy interface {
	Shard(view *view.View, record string) (int32, int32)
}

// ShardingPolicy determines which records are appended to which shards.

type Client struct {
	clientID       int32
	numReplica     int32
	nextCSN        int32
	nextGSN        int32
	viewID         int32
	view           *view.View
	viewC          chan *discpb.View
	appendC        chan *datapb.Record
	ackC           chan *datapb.Ack
	subC           chan *datapb.Record
	shardingPolicy ShardingPolicy

	discAddr   address.DiscAddr
	discConn   *grpc.ClientConn
	discClient *discpb.Discovery_DiscoverClient
	discMu     sync.Mutex

	dataAddr           address.DataAddr
	dataConn           map[int32]*grpc.ClientConn
	dataConnMu         sync.Mutex
	dataAppendClient   map[int32]datapb.Data_AppendClient
	dataAppendClientMu sync.Mutex
}

func NewClient(dataAddr address.DataAddr, discAddr address.DiscAddr, numReplica int32) (*Client, error) {
	c := &Client{
		clientID:   generateClientID(),
		numReplica: numReplica,
		nextCSN:    -1,
		nextGSN:    0,
		viewID:     0,
		dataAddr:   dataAddr,
		discAddr:   discAddr,
	}
	c.shardingPolicy = NewDefaultShardingPolicy(numReplica)
	c.viewC = make(chan *discpb.View, 4096)
	c.appendC = make(chan *datapb.Record, 4096)
	c.ackC = make(chan *datapb.Ack, 4096)
	c.subC = make(chan *datapb.Record, 4096)
	c.dataConn = make(map[int32]*grpc.ClientConn)
	c.dataAppendClient = make(map[int32]datapb.Data_AppendClient)
	c.view = view.NewView()
	err := c.UpdateDiscovery()
	if err != nil {
		return nil, err
	}
	go c.subscribeView()
	return c, nil
}

func generateClientID() int32 {
	seed := rand.NewSource(time.Now().UnixNano())
	return rand.New(seed).Int31()
}

func (c *Client) subscribeView() {
	for {
		v, err := (*c.discClient).Recv()
		if err != nil {
			log.Errorf("%v", err)
			continue
		}
		err = c.view.Update(v)
		if err != nil {
			log.Errorf("%v", err)
		}
	}
}

func (c *Client) UpdateDiscovery() error {
	return c.UpdateDiscoveryAddr(c.discAddr.Get())
}

func (c *Client) UpdateDiscoveryAddr(addr string) error {
	c.discMu.Lock()
	defer c.discMu.Unlock()
	if c.discConn != nil {
		c.discConn.Close()
		c.discConn = nil
	}
	opts := []grpc.DialOption{grpc.WithInsecure(), grpc.WithBlock()}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return fmt.Errorf("Dial discovery at %v failed: %v", addr, err)
	}
	c.discConn = conn
	discClient := discpb.NewDiscoveryClient(conn)
	callOpts := []grpc.CallOption{}
	discDiscoveryClient, err := discClient.Discover(context.Background(), &discpb.Empty{}, callOpts...)
	if err != nil {
		return fmt.Errorf("Create replicate client to %v failed: %v", addr, err)
	}
	c.discClient = &discDiscoveryClient

	v, err := (*c.discClient).Recv()
	if err != nil {
		log.Errorf("%v", err)
	}
	log.Debugf("Discovery updating view: %v", v)
	err = c.view.Update(v)
	if err != nil {
		log.Errorf("%v", err)
	}

	return nil
}

// the caller is responsible to lock the data
func (c *Client) connDataServer(shard, replica int32) (*grpc.ClientConn, error) {
	globalReplicaID := shard*c.numReplica + replica
	addr := c.dataAddr.Get(shard, replica)
	if conn, ok := c.dataConn[globalReplicaID]; ok && conn != nil {
		c.dataConn[globalReplicaID].Close()
		delete(c.dataConn, globalReplicaID)
	}
	opts := []grpc.DialOption{grpc.WithInsecure(), grpc.WithBlock()}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("Dial data server at %v failed: %v", addr, err)
	}
	c.dataConn[globalReplicaID] = conn
	return conn, nil
}

func (c *Client) getDataAppendClient(shard, replica int32) (datapb.Data_AppendClient, error) {
	globalReplicaID := shard*c.numReplica + replica
	c.dataAppendClientMu.Lock()
	defer c.dataAppendClientMu.Unlock()
	if client, ok := c.dataAppendClient[globalReplicaID]; ok && client != nil {
		return client, nil
	}
	return c.buildDataAppendClient(shard, replica)
}

func (c *Client) buildDataAppendClient(shard, replica int32) (datapb.Data_AppendClient, error) {
	globalReplicaID := shard*c.numReplica + replica
	conn, err := c.getDataServerConn(shard, replica)
	if err != nil {
		return nil, err
	}
	dataClient := datapb.NewDataClient(conn)
	dataAppendClient, err := dataClient.Append(context.Background())
	if err != nil {
		return nil, fmt.Errorf("Build data append client of shard %v replica %v failed: %v", shard, replica, err)
	}
	c.dataAppendClient[globalReplicaID] = dataAppendClient
	return dataAppendClient, nil
}

func (c *Client) getDataServerConn(shard, replica int32) (*grpc.ClientConn, error) {
	globalReplicaID := shard*c.numReplica + replica
	c.dataConnMu.Lock()
	defer c.dataConnMu.Unlock()
	if conn, ok := c.dataConn[globalReplicaID]; ok && conn != nil {
		return conn, nil
	}
	return c.connDataServer(shard, replica)
}

func (c *Client) Start() {
	go c.processView()
	go c.processAppend()
	go c.processAck()
}

func (c *Client) processView() {
	for v := range c.viewC {
		log.Debugf("Client: %v", v)
		err := c.view.Update(v)
		if err != nil {
			log.Errorf("%v", err)
		}
	}
}

func (c *Client) processAppend() {
	for r := range c.appendC {
		shard, replica := c.shardingPolicy.Shard(c.view, r.Record)
		r := &datapb.Record{
			ClientID: c.clientID,
			ClientSN: c.getNextClientSN(),
			Record:   r.Record,
		}
		log.Infof("shard: %v, replica: %v\n", shard, replica)
		client, err := c.getDataAppendClient(shard, replica)
		if err != nil {
			log.Errorf("%v", err)
			continue
		}
		err = client.Send(r)
		if err != nil {
			log.Errorf("%v", err)
		}
	}
}

func (c *Client) processAck() {
	for r := range c.ackC {
		_ = r
	}
}

func (c *Client) getNextClientSN() int32 {
	csn := atomic.AddInt32(&c.nextCSN, 1)
	return csn
}

func (c *Client) Append(record string) (int64, int32, error) {
	r := &datapb.Record{
		ClientID: c.clientID,
		ClientSN: c.getNextClientSN(),
		Record:   record,
	}
	c.appendC <- r
	return 0, 0, nil
}

func (c *Client) AppendOne(record string) (int64, int32, error) {
	r := &datapb.Record{
		ClientID: c.clientID,
		ClientSN: c.getNextClientSN(),
		Record:   record,
	}
	shard, replica := c.shardingPolicy.Shard(c.view, record)
	log.Infof("shard: %v, replica: %v\n", shard, replica)
	conn, err := c.getDataServerConn(shard, replica)
	if err != nil {
		return 0, 0, err
	}
	opts := []grpc.CallOption{}
	dataClient := datapb.NewDataClient(conn)
	ack, err := dataClient.AppendOne(context.TODO(), r, opts...)
	if err != nil {
		return 0, 0, err
	}
	return ack.GlobalSN, ack.ShardID, nil
}

func (c *Client) Read(gsn int64, shard, replica int32) (string, error) {
	globalSN := &datapb.GlobalSN{GSN: gsn}
	conn, err := c.getDataServerConn(shard, replica)
	if err != nil {
		return "", err
	}
	opts := []grpc.CallOption{}
	dataClient := datapb.NewDataClient(conn)
	record, err := dataClient.Read(context.TODO(), globalSN, opts...)
	if err != nil {
		return "", err
	}
	return record.Record, nil
}

func (c *Client) SetShardingPolicy(p ShardingPolicy) {
	c.shardingPolicy = p
}
