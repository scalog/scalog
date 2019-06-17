package data

import (
	"context"
	"io"

	"github.com/scalog/scalogger/data/datapb"
	log "github.com/scalog/scalogger/logger"
)

func (server *DataServer) Append(stream datapb.Data_AppendServer) error {
	done := make(chan struct{})
	go server.respondToClient(done, stream)
	for {
		select {
		case <-done:
			return nil
		default:
			record, err := stream.Recv()
			if err != nil {
				close(done)
				if err == io.EOF {
					return nil
				}
				return err
			}
			server.appendC <- record
		}
	}
}

func (server *DataServer) respondToClient(done chan struct{}, stream datapb.Data_AppendServer) {
	ackC := make(chan *datapb.Ack)
	server.ackCMu.Lock()
	cid := server.clientID
	server.clientID++
	server.ackC[cid] = ackC
	server.ackCMu.Unlock()
	for {
		select {
		case <-done:
			server.ackCMu.Lock()
			delete(server.ackC, cid)
			server.ackCMu.Unlock()
			log.Infof("Client %v is closed", cid)
			close(ackC)
			return
		case ack := <-ackC:
			if err := stream.Send(ack); err != nil {
				server.ackCMu.Lock()
				delete(server.ackC, cid)
				server.ackCMu.Unlock()
				log.Infof("Client %v is closed", cid)
				close(ackC)
				close(done)
				return
			}
		}
	}
}

func (server *DataServer) Trim(ctx context.Context, req *datapb.GlobalSN) (*datapb.Empty, error) {
	return &datapb.Empty{}, nil
}
