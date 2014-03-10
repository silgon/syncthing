package main

/* This models the cluster contents.

The model contains a file set for each repository. */

// TODO: Index sending
// TODO: Stats

import (
	"errors"
	"os"
	"path"

	"github.com/calmh/syncthing/buffers"
	"github.com/calmh/syncthing/cid"
	"github.com/calmh/syncthing/files"
	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
)

type model struct {
	dir   map[string]string             // repo name -> directory
	fs    map[string]*files.Set         // repo name -> file set
	cm    map[string]map[string]bool    // repo name -> node ID -> is member
	conns map[string]protocolConnection // node ID -> connection
	im    *cid.Map                      // node ID <-> connection ID

	connectMsg      chan connectMsg
	disconnectMsg   chan disconnectMsg
	initialIndexMsg chan indexMsg
	updateIndexMsg  chan indexMsg
	initialRepoMsg  chan repoMsg
	updateRepoMsg   chan repoMsg
	requestMsg      chan requestMsg
	needMsg         chan needMsg
	optionsMsg      chan optionsMsg
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
	files  []scanner.File
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

type optionsMsg struct {
	nodeID  string
	options map[string]string
}

func newModel() *model {
	m := &model{
		dir:             make(map[string]string),
		fs:              make(map[string]*files.Set),
		conns:           make(map[string]protocolConnection),
		im:              cid.NewMap(),
		connectMsg:      make(chan connectMsg),
		disconnectMsg:   make(chan disconnectMsg),
		initialIndexMsg: make(chan indexMsg),
		updateIndexMsg:  make(chan indexMsg),
		initialRepoMsg:  make(chan repoMsg),
		updateRepoMsg:   make(chan repoMsg),
		requestMsg:      make(chan requestMsg),
		needMsg:         make(chan needMsg),
		optionsMsg:      make(chan optionsMsg),
	}
	m.run()
	return m
}

func (m *model) run() {
	for {
		select {
		case msg := <-m.connectMsg:
			m.conns[msg.conn.ID()] = msg.conn
			// Send initial index for all repos
			for repo, fileset := range fs {
				// TODO: Only for repos where the connection is a member
				idx := fileInfosFromFiles(fileset.Have(0))
				msg.conn.Index(repo, idx)
			}
			// TODO: Start whatever needed to service the conn

		case msg := <-m.disconnectMsg:
			cid := uint(m.im.Get(msg.nodeID))
			for _, repo := range m.fs {
				repo.SetRemote(cid, nil)
			}

			m.im.Clear(msg.nodeID)
			delete(m.conns, msg.nodeID)

		case req := <-m.requestMsg:
			if m.fs[req.repo].Availability(req.name)&1 != 1 {
				req.responseMsg <- responseMsg{data: nil, err: errUnavailable}
				continue
			}
			m.handleRequest(req)

		case idx := <-m.initialIndexMsg:
			cid := uint(m.im.Get(idx.nodeID))
			// TODO: Make the conversion more efficient
			fsf := fsFilesFromFiles(filesFromFileInfos(idx.files))
			repo := m.fs[idx.repo]
			repo.SetRemote(cid, fsf)

		case idx := <-m.updateIndexMsg:
			cid := uint(m.im.Get(idx.nodeID))
			fsf := fsFilesFromFiles(filesFromFileInfos(idx.files))
			repo := m.fs[idx.repo]
			repo.AddRemote(cid, fsf)

		case msg := <-m.initialRepoMsg:
			fsf := fsFilesFromFiles(msg.files)
			repo := m.fs[msg.repo]
			repo.SetLocal(fsf)

		case msg := <-m.updateRepoMsg:
			// TODO: Delete records
			fsf := fsFilesFromFiles(msg.files)
			repo := m.fs[msg.repo]
			repo.AddLocal(fsf)

		case msg := <-m.optionsMsg:
			_ = msg
		}
	}
}

func (m *model) AddConnection(conn protocolConnection) {
	m.connectMsg <- connectMsg{conn: conn}
}

func (m *model) handleRequest(req requestMsg) {
	fn := path.Join(m.dir[req.repo], req.name)

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
