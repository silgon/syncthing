package main

import "github.com/calmh/syncthing/protocol"

// The connectionDelegate type implement the protocol.Model interface on behalf of the model.
type connectionDelegate struct {
	models map[string]*model // maps repo -> model
}

func (c connectionDelegate) Index(nodeID, repo string, fs []protocol.FileInfo) {
	if model, ok := c.models[repo]; ok {
		model.initialIndexMsg <- indexMsg{
			repo:   "default",
			nodeID: nodeID,
			files:  fs,
		}
	} else {
		warnf("Index message from %q for unknown repo %q", nodeID, repo)
	}
}

func (c connectionDelegate) IndexUpdate(nodeID, repo string, fs []protocol.FileInfo) {
	if model, ok := c.models[repo]; ok {
		model.updateIndexMsg <- indexMsg{
			repo:   "default",
			nodeID: nodeID,
			files:  fs,
		}
	} else {
		warnf("IndexUpdate message from %q for unknown repo %q", nodeID, repo)
	}
}

func (c connectionDelegate) Close(node string, err error) {
	for _, model := range c.models {
		model.disconnectMsg <- disconnectMsg{node}
	}
}

func (c connectionDelegate) Request(nodeID, repo, name string, offset int64, size int) ([]byte, error) {
	if model, ok := c.models[repo]; ok {
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
	} else {
		warnf("Request message from %q for unknown repo %q", nodeID, repo)
		return nil, errUnavailable
	}
}
