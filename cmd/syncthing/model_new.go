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
	repoDirsMsg     chan chan [][2]string
}

var errUnavailable = errors.New("file unavailable")

// This is a protocol.Connection, broken out to an interface to make testing easier
type protocolConnection interface {
	ID() string
	Index(repo string, files []protocol.FileInfo)
	Request(repo, name string, offset int64, size int) ([]byte, error)
	Statistics() protocol.Statistics
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
	repo  string
	files []scanner.File
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
		cm:              make(map[string]map[string]bool),
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
		repoDirsMsg:     make(chan chan [][2]string),
	}
	m.run()
	return m
}

func (m *model) run() {
	for {
		select {
		case msg := <-m.connectMsg:
			if debugModel {
				dlog.Printf("%#v", msg)
			}
			nodeID := msg.conn.ID()
			m.conns[nodeID] = msg.conn
			// Send initial index for all repos
			for repo, fileset := range m.fs {
				if m.cm[repo][nodeID] {
					idx := scannerToProtocolSlice(filesToScannerSlice(fileset.Have(0)))
					msg.conn.Index(repo, idx)
				}
			}
			// TODO: Start whatever needed to service the conn

		case msg := <-m.disconnectMsg:
			if debugModel {
				dlog.Printf("%#v", msg)
			}
			cid := uint(m.im.Get(msg.nodeID))
			for _, repo := range m.fs {
				repo.SetRemote(cid, nil)
			}

			m.im.Clear(msg.nodeID)
			delete(m.conns, msg.nodeID)

		case req := <-m.requestMsg:
			if debugModel {
				dlog.Printf("%#v", req)
			}
			if m.fs[req.repo].Availability(req.name)&1 != 1 {
				req.responseMsg <- responseMsg{data: nil, err: errUnavailable}
				continue
			}
			m.handleRequest(req)

		case idx := <-m.initialIndexMsg:
			if debugModel {
				dlog.Printf("initialIndex")
			}
			cid := uint(m.im.Get(idx.nodeID))
			// TODO: Make the conversion more efficient
			fsf := scannerToFilesSlice(protocolToScannerSlice(idx.files))
			repo := m.fs[idx.repo]
			repo.SetRemote(cid, fsf)

		case idx := <-m.updateIndexMsg:
			if debugModel {
				dlog.Printf("updateIndex")
			}
			cid := uint(m.im.Get(idx.nodeID))
			fsf := scannerToFilesSlice(protocolToScannerSlice(idx.files))
			repo := m.fs[idx.repo]
			repo.AddRemote(cid, fsf)

		case msg := <-m.initialRepoMsg:
			if debugModel {
				dlog.Printf("initialRepo")
			}
			fsf := scannerToFilesSlice(msg.files)
			repo := m.fs[msg.repo]
			repo.SetLocal(fsf)

		case msg := <-m.updateRepoMsg:
			if debugModel {
				dlog.Printf("updateRepo")
			}
			// TODO: Delete records
			fsf := scannerToFilesSlice(msg.files)
			repo := m.fs[msg.repo]
			repo.AddLocal(fsf)

		case msg := <-m.optionsMsg:
			if debugModel {
				dlog.Printf("%#v", msg)
			}
			_ = msg

		case ch := <-m.repoDirsMsg:
			if debugModel {
				dlog.Printf("repoDirs")
			}
			var repoDirs [][2]string
			for repo, dir := range m.dir {
				repoDirs = append(repoDirs, [2]string{repo, dir})
			}
			ch <- repoDirs
		}
	}
}

func (m *model) AddConnection(conn protocolConnection) {
	m.connectMsg <- connectMsg{conn: conn}
}

func (m *model) AddRepository(repo, dir string, nodes []string) {
}

func (m *model) InitialRepoContents(repo string, files []scanner.File) {
	m.initialRepoMsg <- repoMsg{
		repo:  repo,
		files: files,
	}
}

func (m *model) UpdateRepoContents(repo string, files []scanner.File) {
	m.updateRepoMsg <- repoMsg{
		repo:  repo,
		files: files,
	}
}

func (m *model) LimitSendRate(kbps int) {
}

func (m *model) StartRW(del bool, par int) {
}

func (m *model) ConnectedTo(nodeID string) bool {
	return false // implement conman
}

// RepoDirs returns a slice of [repo, dir] arrays.
func (m *model) RepoDirs() [][2]string {
	c := make(chan [][2]string)
	m.repoDirsMsg <- c
	return <-c
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
