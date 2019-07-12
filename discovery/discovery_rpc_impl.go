package discovery

import (
	"io"

	"github.com/scalog/scalog/discovery/discpb"
	log "github.com/scalog/scalog/logger"
)

func (s *DiscoveryServer) Discover(empty *discpb.Empty, stream discpb.Discovery_DiscoverServer) error {
	viewC := make(chan *discpb.View, 4096)
	s.viewCMu.Lock()
	cid := s.clientID
	s.viewC[cid] = viewC
	s.clientID++
	s.viewCMu.Unlock()
	// clean up before returning
	defer func() {
		s.viewCMu.Lock()
		delete(s.viewC, cid)
		close(viewC)
		s.viewCMu.Unlock()
	}()
	// send new views to the client
	v := s.getView()
	err := stream.Send(v)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		log.Warningf("%v", err)
		return err
	}
	for v := range viewC {
		err := stream.Send(v)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			log.Warningf("%v", err)
			return err
		}
	}
	return nil
}
