package order

import (
	"context"
	"io"

	log "github.com/scalog/scalog/logger"
	"github.com/scalog/scalog/order/orderpb"
)

func (s *OrderServer) Report(stream orderpb.Order_ReportServer) error {
	done := make(chan struct{})
	go s.respondToDataReplica(done, stream)
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
			s.forwardC <- req
		}
	}
}

func (s *OrderServer) respondToDataReplica(done chan struct{}, stream orderpb.Order_ReportServer) {
	respC := make(chan *orderpb.CommittedEntry, 4096)
	s.subCMu.Lock()
	cid := s.clientID
	s.clientID++
	s.subC[cid] = respC
	s.subCMu.Unlock()
	for {
		select {
		case <-done:
			s.subCMu.Lock()
			delete(s.subC, cid)
			s.subCMu.Unlock()
			log.Infof("Client %v is closed", cid)
			close(respC)
			return
		case resp := <-respC:
			if err := stream.Send(resp); err != nil {
				s.subCMu.Lock()
				delete(s.subC, cid)
				s.subCMu.Unlock()
				log.Infof("Client %v is closed", cid)
				close(respC)
				close(done)
				return
			}
		}
	}
}

func (s *OrderServer) Forward(stream orderpb.Order_ForwardServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		s.forwardC <- req
	}
}

func (s *OrderServer) Finalize(ctx context.Context, req *orderpb.FinalizeEntry) (*orderpb.Empty, error) {
	s.finalizeC <- req
	return &orderpb.Empty{}, nil
}
