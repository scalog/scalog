package order

import (
	"context"
	"io"

	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/order/orderpb"
)

func (server *OrderServer) Report(stream orderpb.Order_ReportServer) error {
	done := make(chan struct{})
	go server.respondToDataReplica(done, stream)
	for {
		select {
		case <-done:
			return nil
		default:
			req, err := stream.Recv()
			if err != nil {
				close(done)
				if err == io.EOF {
					return nil
				}
				return err
			}
			server.forwardC <- req
		}
	}
}

func (server *OrderServer) respondToDataReplica(done chan struct{}, stream orderpb.Order_ReportServer) {
	respC := make(chan *orderpb.CommittedEntry, 4096)
	server.subCMu.Lock()
	cid := server.clientID
	server.clientID++
	server.subC[cid] = respC
	server.subCMu.Unlock()
	for {
		select {
		case <-done:
			server.subCMu.Lock()
			delete(server.subC, cid)
			server.subCMu.Unlock()
			log.Infof("Client %v is closed", cid)
			close(respC)
			return
		case resp := <-respC:
			if err := stream.Send(resp); err != nil {
				server.subCMu.Lock()
				delete(server.subC, cid)
				server.subCMu.Unlock()
				log.Infof("Client %v is closed", cid)
				close(respC)
				close(done)
				return
			}
		}
	}
}

func (server *OrderServer) Forward(stream orderpb.Order_ForwardServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		server.forwardC <- req
	}
}

func (server *OrderServer) Finalize(ctx context.Context, req *orderpb.FinalizeEntry) (*orderpb.Empty, error) {
	server.finalizeC <- req
	return &orderpb.Empty{}, nil
}
