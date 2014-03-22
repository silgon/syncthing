package main

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"sync"
	"time"

	"github.com/calmh/syncthing/buffers"
	"github.com/calmh/syncthing/cid"
	"github.com/calmh/syncthing/files"
	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
)

type Model struct {
	dir  string
	cm   *cid.Map
	fs   *files.Set
	fmut sync.Mutex

	protoConn map[string]Connection
	rawConn   map[string]io.Closer
	pmut      sync.RWMutex // protects protoConn and rawConn

	bq *blockQueue
	dq chan scanner.File // queue for files to delete

	rwRunning bool
	delete    bool
	initmut   sync.Mutex // protects rwRunning and delete

	sup suppressor

	parallelRequests int
	limitRequestRate chan struct{}

	imut sync.Mutex // protects Index
}

type Connection interface {
	ID() string
	Index(string, []protocol.FileInfo)
	Request(repo, name string, offset int64, size int) ([]byte, error)
	Statistics() protocol.Statistics
	Option(key string) string
}

const (
	idxBcastHoldtime = 15 * time.Second  // Wait at least this long after the last index modification
	idxBcastMaxDelay = 120 * time.Second // Unless we've already waited this long
)

var (
	ErrNoSuchFile = errors.New("no such file")
	ErrInvalid    = errors.New("file is invalid")
)

// NewModel creates and starts a new model. The model starts in read-only mode,
// where it sends index information to connected peers and responds to requests
// for file data without altering the local repository in any way.
func NewModel(dir string, maxChangeBw int) *Model {
	m := &Model{
		dir:       dir,
		cm:        cid.NewMap(),
		fs:        files.NewSet(),
		protoConn: make(map[string]Connection),
		rawConn:   make(map[string]io.Closer),
		bq:        newBlockQueue(),
		dq:        make(chan scanner.File),
		sup:       suppressor{threshold: int64(maxChangeBw)},
	}

	go m.broadcastIndexLoop()
	return m
}

func (m *Model) LimitRate(kbps int) {
	m.limitRequestRate = make(chan struct{}, kbps)
	n := kbps/10 + 1
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			for i := 0; i < n; i++ {
				select {
				case m.limitRequestRate <- struct{}{}:
				}
			}
		}
	}()
}

// StartRW starts read/write processing on the current model. When in
// read/write mode the model will attempt to keep in sync with the cluster by
// pulling needed files from peer nodes.
func (m *Model) StartRW(del bool, threads int) {
	m.initmut.Lock()
	defer m.initmut.Unlock()

	if m.rwRunning {
		panic("starting started model")
	}

	m.rwRunning = true
	m.delete = del
	m.parallelRequests = threads

	if del {
		go m.deleteLoop()
	}
}

// Generation returns an opaque integer that is guaranteed to increment on
// every change to the local repository or global model.
func (m *Model) Generation() int64 {
	m.fmut.Lock()
	c := m.fs.Changes()
	m.fmut.Unlock()
	return c
}

type ConnectionInfo struct {
	protocol.Statistics
	Address       string
	ClientID      string
	ClientVersion string
	Completion    int
}

// ConnectionStats returns a map with connection statistics for each connected node.
func (m *Model) ConnectionStats() map[string]ConnectionInfo {
	type remoteAddrer interface {
		RemoteAddr() net.Addr
	}

	m.fmut.Lock()
	m.pmut.RLock()

	var tot int64
	for _, f := range m.fs.Global() {
		if f.Flags&protocol.FlagDeleted == 0 {
			tot += f.Size
		}
	}

	var res = make(map[string]ConnectionInfo)
	for node, conn := range m.protoConn {
		ci := ConnectionInfo{
			Statistics:    conn.Statistics(),
			ClientID:      conn.Option("clientId"),
			ClientVersion: conn.Option("clientVersion"),
		}
		if nc, ok := m.rawConn[node].(remoteAddrer); ok {
			ci.Address = nc.RemoteAddr().String()
		}

		var have = tot
		for _, f := range m.fs.Need(m.cm.Get(node)) {
			if f.Flags&protocol.FlagDeleted == 0 {
				have -= f.Size
			}
		}

		ci.Completion = 100
		if tot != 0 {
			ci.Completion = int(100 * have / tot)
		}

		res[node] = ci
	}

	m.pmut.RUnlock()
	m.fmut.Unlock()

	return res
}

func sizeOf(fs []scanner.File) (files, deleted int, bytes int64) {
	for _, f := range fs {
		if f.Flags&protocol.FlagDeleted == 0 {
			files++
			bytes += f.Size
		} else {
			deleted++
		}
	}
	return
}

// GlobalSize returns the number of files, deleted files and total bytes for all
// files in the global model.
func (m *Model) GlobalSize() (files, deleted int, bytes int64) {
	m.fmut.Lock()
	fs := m.fs.Global()
	m.fmut.Unlock()
	return sizeOf(fs)
}

// LocalSize returns the number of files, deleted files and total bytes for all
// files in the local repository.
func (m *Model) LocalSize() (files, deleted int, bytes int64) {
	m.fmut.Lock()
	fs := m.fs.Have(cid.LocalID)
	m.fmut.Unlock()
	return sizeOf(fs)
}

// InSyncSize returns the number and total byte size of the local files that
// are in sync with the global model.
func (m *Model) InSyncSize() (files int, bytes int64) {
	m.fmut.Lock()
	gf := m.fs.Global()
	hf := m.fs.Need(cid.LocalID)
	m.fmut.Unlock()

	gn, _, gb := sizeOf(gf)
	hn, _, hb := sizeOf(hf)

	return gn - hn, gb - hb
}

// NeedFiles returns the list of currently needed files and the total size.
func (m *Model) NeedFiles() ([]scanner.File, int64) {
	m.fmut.Lock()
	nf := m.fs.Need(cid.LocalID)
	m.fmut.Unlock()

	var bytes int64
	for _, f := range nf {
		bytes += f.Size
	}

	return nf, bytes
}

// Index is called when a new node is connected and we receive their full index.
// Implements the protocol.Model interface.
func (m *Model) Index(nodeID string, fs []protocol.FileInfo) {
	var files = make([]scanner.File, len(fs))
	for i := range fs {
		files[i] = fileFromFileInfo(fs[i])
	}

	m.fmut.Lock()
	cid := m.cm.Get(nodeID)
	m.fs.SetRemote(cid, files)
	m.queueNeededBlocks()
	m.fmut.Unlock()

	if debugNet {
		dlog.Printf("IDX(in): %s: %d files", nodeID, len(fs))
	}
}

// IndexUpdate is called for incremental updates to connected nodes' indexes.
// Implements the protocol.Model interface.
func (m *Model) IndexUpdate(nodeID string, fs []protocol.FileInfo) {
	var files = make([]scanner.File, len(fs))
	for i := range fs {
		files[i] = fileFromFileInfo(fs[i])
	}

	m.fmut.Lock()
	id := m.cm.Get(nodeID)
	m.fs.AddRemote(id, files)
	m.queueNeededBlocks()
	m.fmut.Unlock()

	if debugNet {
		dlog.Printf("IDXUP(in): %s: %d files", nodeID, len(files))
	}
}

func (m *Model) queueNeededBlocks() {
	for _, f := range m.fs.Need(cid.LocalID) {
		if !m.bq.contains(f.Name) {
			gf := m.fs.GetGlobal(f.Name)
			_, need := scanner.BlockDiff(f.Blocks, gf.Blocks)
			l := len(need)
			var offset int64
			for i, b := range need {
				m.bq.put(queuedBlock{
					file:   f.Name,
					offset: offset,
					size:   b.Size,
					last:   i == l-1,
				})
				offset += int64(b.Size)
			}
		}
	}
}

// Close removes the peer from the model and closes the underlying connection if possible.
// Implements the protocol.Model interface.
func (m *Model) Close(node string, err error) {
	if debugNet {
		dlog.Printf("%s: %v", node, err)
	}
	if err == protocol.ErrClusterHash {
		warnf("Connection to %s closed due to mismatched cluster hash. Ensure that the configured cluster members are identical on both nodes.", node)
	} else if err != io.EOF {
		warnf("Connection to %s closed: %v", node, err)
	}

	m.fmut.Lock()
	cid := m.cm.Get(node)
	m.fs.SetRemote(cid, nil)
	m.cm.Clear(node)
	m.fmut.Unlock()

	m.pmut.Lock()
	conn, ok := m.rawConn[node]
	if ok {
		conn.Close()
	}
	delete(m.protoConn, node)
	delete(m.rawConn, node)
	m.pmut.Unlock()
}

// Request returns the specified data segment by reading it from local disk.
// Implements the protocol.Model interface.
func (m *Model) Request(nodeID, repo, name string, offset int64, size int) ([]byte, error) {
	// Verify that the requested file exists in the local model.
	m.fmut.Lock()
	lf := m.fs.Get(cid.LocalID, name)
	m.fmut.Unlock()
	if offset > lf.Size {
		warnf("SECURITY (nonexistent file) REQ(in): %s: %q o=%d s=%d", nodeID, name, offset, size)
		return nil, ErrNoSuchFile
	}
	if lf.Suppressed {
		return nil, ErrInvalid
	}

	if debugNet && nodeID != "<local>" {
		dlog.Printf("REQ(in): %s: %q o=%d s=%d", nodeID, name, offset, size)
	}
	fn := path.Join(m.dir, name)
	fd, err := os.Open(fn) // XXX: Inefficient, should cache fd?
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	buf := buffers.Get(int(size))
	_, err = fd.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}

	if m.limitRequestRate != nil {
		for s := 0; s < len(buf); s += 1024 {
			<-m.limitRequestRate
		}
	}

	return buf, nil
}

// ReplaceLocal replaces the local repository index with the given list of files.
func (m *Model) ReplaceLocal(fs []scanner.File) {
	m.fmut.Lock()
	m.fs.SetLocal(fs)
	m.fmut.Unlock()
}

// ReplaceLocal replaces the local repository index with the given list of files.
func (m *Model) SeedLocal(fs []protocol.FileInfo) {
	var sfs = make([]scanner.File, len(fs))
	for i := 0; i < len(fs); i++ {
		sfs[i] = fileFromFileInfo(fs[i])
	}

	m.fmut.Lock()
	m.fs.SetLocalNoDelete(sfs)
	m.fmut.Unlock()
}

// Implements scanner.CurrentFiler
func (m *Model) CurrentFile(file string) scanner.File {
	m.fmut.Lock()
	f := m.fs.Get(cid.LocalID, file)
	m.fmut.Unlock()
	return f
}

// ConnectedTo returns true if we are connected to the named node.
func (m *Model) ConnectedTo(nodeID string) bool {
	m.pmut.RLock()
	_, ok := m.protoConn[nodeID]
	m.pmut.RUnlock()
	return ok
}

// RepoID returns a unique ID representing the current repository location.
func (m *Model) RepoID() string {
	return fmt.Sprintf("%x", sha1.Sum([]byte(m.dir)))
}

// AddConnection adds a new peer connection to the model. An initial index will
// be sent to the connected peer, thereafter index updates whenever the local
// repository changes.
func (m *Model) AddConnection(rawConn io.Closer, protoConn Connection) {
	nodeID := protoConn.ID()
	m.pmut.Lock()
	m.protoConn[nodeID] = protoConn
	m.rawConn[nodeID] = rawConn
	m.pmut.Unlock()

	go func() {
		idx := m.ProtocolIndex()
		if debugNet {
			dlog.Printf("IDX(out/initial): %s: %d files", nodeID, len(idx))
		}
		protoConn.Index("default", idx)
	}()
}

// ProtocolIndex returns the current local index in protocol data types.
// Must be called with the read lock held.
func (m *Model) ProtocolIndex() []protocol.FileInfo {
	var index []protocol.FileInfo

	m.fmut.Lock()
	fs := m.fs.Have(cid.LocalID)
	m.fmut.Unlock()

	for _, f := range fs {
		mf := fileInfoFromFile(f)
		if debugIdx {
			var flagComment string
			if mf.Flags&protocol.FlagDeleted != 0 {
				flagComment = " (deleted)"
			}
			dlog.Printf("IDX(out): %q m=%d f=%o%s v=%d (%d blocks)", mf.Name, mf.Modified, mf.Flags, flagComment, mf.Version, len(mf.Blocks))
		}
		index = append(index, mf)
	}

	return index
}

func (m *Model) updateLocal(f scanner.File) {
	m.fmut.Lock()
	m.fs.AddLocal([]scanner.File{f})
	m.fmut.Unlock()
}

func (m *Model) requestGlobal(nodeID, name string, offset int64, size int, hash []byte) ([]byte, error) {
	m.pmut.RLock()
	nc, ok := m.protoConn[nodeID]
	m.pmut.RUnlock()

	if !ok {
		return nil, fmt.Errorf("requestGlobal: no such node: %s", nodeID)
	}

	if debugNet {
		dlog.Printf("REQ(out): %s: %q o=%d s=%d h=%x", nodeID, name, offset, size, hash)
	}

	return nc.Request("default", name, offset, size)
}

func (m *Model) broadcastIndexLoop() {
	var lastChange int64
	for {
		time.Sleep(5 * time.Second)

		m.fmut.Lock()
		c := m.fs.Changes()
		if c == lastChange {
			m.fmut.Unlock()
			continue
		}
		lastChange = c
		fs := m.fs.Have(cid.LocalID)
		m.fmut.Unlock()

		var indexWg sync.WaitGroup
		indexWg.Add(len(m.protoConn))

		var idx = make([]protocol.FileInfo, len(fs))
		for i, f := range fs {
			idx[i] = fileInfoFromFile(f)
		}

		m.pmut.RLock()
		for _, node := range m.protoConn {
			node := node
			if debugNet {
				dlog.Printf("IDX(out/loop): %s: %d files", node.ID(), len(idx))
			}
			go func() {
				node.Index("default", idx)
				indexWg.Done()
			}()
		}
		m.pmut.RUnlock()

		indexWg.Wait()
	}
}

func (m *Model) deleteLoop() {
	for file := range m.dq {
		if debugPull {
			dlog.Println("delete", file.Name)
		}
		path := FSNormalize(path.Clean(path.Join(m.dir, file.Name)))
		err := os.Remove(path)
		if err != nil {
			warnf("%s: %v", file.Name, err)
		}

		m.updateLocal(file)
	}
}

func fileFromFileInfo(f protocol.FileInfo) scanner.File {
	var blocks = make([]scanner.Block, len(f.Blocks))
	var offset int64
	for i, b := range f.Blocks {
		blocks[i] = scanner.Block{
			Offset: offset,
			Size:   b.Size,
			Hash:   b.Hash,
		}
		offset += int64(b.Size)
	}
	return scanner.File{
		Name:       f.Name,
		Size:       offset,
		Flags:      f.Flags &^ protocol.FlagInvalid,
		Modified:   f.Modified,
		Version:    f.Version,
		Blocks:     blocks,
		Suppressed: f.Flags&protocol.FlagInvalid != 0,
	}
}

func fileInfoFromFile(f scanner.File) protocol.FileInfo {
	var blocks = make([]protocol.BlockInfo, len(f.Blocks))
	for i, b := range f.Blocks {
		blocks[i] = protocol.BlockInfo{
			Size: b.Size,
			Hash: b.Hash,
		}
	}
	pf := protocol.FileInfo{
		Name:     f.Name,
		Flags:    f.Flags,
		Modified: f.Modified,
		Version:  f.Version,
		Blocks:   blocks,
	}
	if f.Suppressed {
		pf.Flags |= protocol.FlagInvalid
	}
	return pf
}
