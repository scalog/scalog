package discovery

import (
	"io"

	"github.com/scalog/scalog/discovery/discpb"
	log "github.com/scalog/scalog/logger"
)

func (server *DiscoveryServer) Discover(empty *discpb.Empty, stream discpb.Discovery_DiscoverServer) error {
	viewC := make(chan *discpb.View, 4096)
	server.viewCMu.Lock()
	cid := server.clientID
	server.viewC[cid] = viewC
	server.clientID++
	server.viewCMu.Unlock()
	// clean up before returning
	defer func() {
		server.viewCMu.Lock()
		delete(server.viewC, cid)
		close(viewC)
		server.viewCMu.Unlock()
	}()
	// send new views to the client
	v := server.getView()
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
