//+build ignore

package main

import (
	"github.com/calmh/syncthing/cid"
	"github.com/calmh/syncthing/files"
	"github.com/calmh/syncthing/protocol"
)

type Model struct {
	connectMsg      chan connectMsg
	disconnectMsg   chan disconnectMsg
	initialIndexMsg chan initialIndexMsg
	updateIndexMsg  chan initialIndexMsg
	initialRepoMsg  chan initialRepoMsg
	updateRepoMsg   chan initialRepoMsg
	requestMsg      chan requestMsg
	needMsg         chan needMsg

	conns map[string]protocolConnection

	im *cid.Map
	fs *files.Set
}

// This is a protocol.Connection, broken out to an interface to make testing easier
type protocolConnection interface {
	ID() string
	Index(string, []protocol.FileInfo)
	Request(repo, name string, offset int64, size int) ([]byte, error)
	Statistics() protocol.Statistics
	Option(key string) string
}

type connectMsg struct {
	conn protocolConnection
}
type disconnectMsg struct {
	nodeID string
}
type initialIndexMsg struct {
	nodeID string
	repo   string
	files  []protocol.FileInfo
}
type updateIndexMsg struct{}
type initialRepoMsg struct {
	nodeID string
	repo   string
	files  []File
}
type needMsg struct{}
type requestMsg struct {
	repo        string
	name        string
	offset      int64
	size        int
	responseMsg chan responseMsg
}
type responseMsg struct {
	data []byte
	err  error
}

func (m *Model) run() {
	for {
		select {
		case msg := <-m.connectMsg:
			m.conns[msg.conn.ID()] = msg.conn
			// TODO: Start whatever needed to service the conn

		case msg := <-m.disconnectMsg:
			// TODO: Stop whatever needed on the conn
			cid := m.im.Get(msg.nodeID)
			m.fs.SetRemote(cid, nil)
			m.im.Clear(msg.nodeID)
			delete(m.conns, msg.nodeID)

		case req := <-m.requestMsg:
			var data []byte
			var err error
			// TODO: read data
			req.responseMsg <- responseMsg{data: data, err: err}

		case idx := <-m.initialIndexMsg:
			cid := m.im.Get(idx.nodeID)
			m.fs.SetRemote(cid, idx.files)

		case idx := <-m.updateIndexMsg:
			cid := m.im.Get(idx.nodeID)
			m.fs.AddRemote(cid, idx.files)

		case repo := <-m.initialRepoMsg:
			m.fs.SetLocal(repo.files)

		case repo := <-m.updateRepoMsg:
			m.fs.AddLocal(repo.files)
		}
	}
}

// The connectionDelegate type implement the protocol.Model interface on behalf of the model.
type connectionDelegate struct {
	model *model
}

func (c connectionDelegate) Index(nodeID string, fs []protocol.FileInfo) {
	c.model.initialIndexMsg <- indexMsg{
		repo:   "default",
		nodeID: nodeID,
		files:  fs,
	}
}

func (c connectionDelegate) IndexUpdate(nodeID string, fs []protocol.FileInfo) {
	c.model.updateIndexMsg <- indexMsg{
		repo:   "default",
		nodeID: nodeID,
		files:  fs,
	}
}

func (c connectionDelegate) Close(node string, err error) {
	c.model.disconnectMsg <- c.nodeID
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
	return rc.data, rc.err
}
