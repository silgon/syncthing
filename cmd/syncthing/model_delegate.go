package main

import "github.com/calmh/syncthing/protocol"

// The connectionDelegate type implement the protocol.Model interface on behalf of the model.
type connectionDelegate struct {
	model *model
}

func (c connectionDelegate) Index(nodeID, repo string, fs []protocol.FileInfo) {
	c.model.initialIndexMsg <- indexMsg{
		repo:   "default",
		nodeID: nodeID,
		files:  fs,
	}
}

func (c connectionDelegate) IndexUpdate(nodeID, repo string, fs []protocol.FileInfo) {
	c.model.updateIndexMsg <- indexMsg{
		repo:   "default",
		nodeID: nodeID,
		files:  fs,
	}
}

func (c connectionDelegate) Close(node string, err error) {
	c.model.disconnectMsg <- disconnectMsg{node}
}

func (c connectionDelegate) Request(nodeID, repo, name string, offset int64, size int) ([]byte, error) {
	rc := make(chan responseMsg)

	c.model.requestMsg <- requestMsg{
		repo:        repo,
		name:        name,
		offset:      offset,
		size:        size,
		responseMsg: rc,
	}

	resp := <-rc
	return resp.data, resp.err
}
