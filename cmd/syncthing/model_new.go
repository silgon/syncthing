package main

/* This models the cluster contents for a given repository. */

import (
	"errors"
	"os"
	"path"

	"github.com/calmh/syncthing/buffers"
	"github.com/calmh/syncthing/cid"
	"github.com/calmh/syncthing/files"
	"github.com/calmh/syncthing/protocol"
)

type model struct {
	dir string

	connectMsg      chan connectMsg
	disconnectMsg   chan disconnectMsg
	initialIndexMsg chan indexMsg
	updateIndexMsg  chan indexMsg
	initialRepoMsg  chan repoMsg
	updateRepoMsg   chan repoMsg
	requestMsg      chan requestMsg
	needMsg         chan needMsg

	conns map[string]protocolConnection

	im *cid.Map
	fs *files.Set
}

var errUnavailable = errors.New("file unavailable")

// This is a protocol.Connection, broken out to an interface to make testing easier
type protocolConnection interface {
	ID() string
	Index(repo string, files []protocol.FileInfo)
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

type indexMsg struct {
	nodeID string
	repo   string
	files  []protocol.FileInfo
}

type repoMsg struct {
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

func (m *model) run() {
	for {
		select {
		case msg := <-m.connectMsg:
			m.conns[msg.conn.ID()] = msg.conn
			// TODO: Send initial index
			// TODO: Start whatever needed to service the conn

		case msg := <-m.disconnectMsg:
			cid := uint(m.im.Get(msg.nodeID))
			m.fs.SetRemote(cid, nil)

			m.im.Clear(msg.nodeID)
			delete(m.conns, msg.nodeID)

		case req := <-m.requestMsg:
			if m.fs.Availability(req.name)&1 != 1 {
				req.responseMsg <- responseMsg{data: nil, err: errUnavailable}
				continue
			}
			m.handleRequest(req)

		case idx := <-m.initialIndexMsg:
			cid := uint(m.im.Get(idx.nodeID))
			// TODO: Make the conversion more efficient
			m.fs.SetRemote(cid, fsFilesFromFiles(filesFromFileInfos(idx.files)))

		case idx := <-m.updateIndexMsg:
			cid := uint(m.im.Get(idx.nodeID))
			m.fs.AddRemote(cid, fsFilesFromFiles(filesFromFileInfos(idx.files)))

		case repo := <-m.initialRepoMsg:
			m.fs.SetLocal(fsFilesFromFiles(repo.files))

		case repo := <-m.updateRepoMsg:
			m.fs.AddLocal(fsFilesFromFiles(repo.files))
		}
	}
}

func (m *model) handleRequest(req requestMsg) {
	fn := path.Join(m.dir, req.name)

	fd, err := os.Open(fn) // TODO: Cache fd
	if err != nil {
		req.responseMsg <- responseMsg{err: err}
		return
	}
	defer fd.Close()

	buf := buffers.Get(int(req.size))
	_, err = fd.ReadAt(buf, req.offset)
	if err != nil {
		req.responseMsg <- responseMsg{err: err}
		return
	}

	req.responseMsg <- responseMsg{data: buf}
}
