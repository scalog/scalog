package order

import (
	"context"
	"io"

	"github.com/scalog/scalogger/order/orderpb"
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
	respC := make(chan *orderpb.CommittedEntry)
	server.subCMu.Lock()
	server.subC = append(server.subC, respC)
	server.subCMu.Unlock()
	for {
		select {
		case <-done:
			return
		case resp := <-respC:
			if err := stream.Send(resp); err != nil {
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
