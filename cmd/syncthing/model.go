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
	fs   files.Set
	fmut sync.Mutex

	global    map[string]scanner.File // the latest version of each file as it exists in the cluster
	gmut      sync.RWMutex            // protects global
	local     map[string]scanner.File // the files we currently have locally on disk
	lmut      sync.RWMutex            // protects local
	remote    map[string]map[string]scanner.File
	rmut      sync.RWMutex // protects remote
	protoConn map[string]Connection
	rawConn   map[string]io.Closer
	pmut      sync.RWMutex // protects protoConn and rawConn

	// Queue for files to fetch. fq can call back into the model, so we must ensure
	// to hold no locks when calling methods on fq.
	fq *FileQueue
	dq chan scanner.File // queue for files to delete

	updatedLocal        int64 // timestamp of last update to local
	updateGlobal        int64 // timestamp of last update to remote
	lastIdxBcast        time.Time
	lastIdxBcastRequest time.Time
	umut                sync.RWMutex // provides updated* and lastIdx*

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
		dir:          dir,
		cm:           cid.NewMap(),
		fs:           files.NewSet(),
		global:       make(map[string]scanner.File),
		local:        make(map[string]scanner.File),
		remote:       make(map[string]map[string]scanner.File),
		protoConn:    make(map[string]Connection),
		rawConn:      make(map[string]io.Closer),
		lastIdxBcast: time.Now(),
		sup:          suppressor{threshold: int64(maxChangeBw)},
		fq:           NewFileQueue(),
		dq:           make(chan scanner.File),
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
	m.umut.RLock()
	defer m.umut.RUnlock()

	return m.updatedLocal + m.updateGlobal
}

func (m *Model) LocalAge() float64 {
	m.umut.RLock()
	defer m.umut.RUnlock()

	return time.Since(time.Unix(m.updatedLocal, 0)).Seconds()
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
	m.rmut.RLock()

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

	m.rmut.RUnlock()
	m.pmut.RUnlock()
	m.fmut.Unlock()

	return res
}

func sizeOf(fs []File) (files, deleted int, bytes int64) {
	for _, f := range fs {
		if f.Flags&protocol.FlagDeleted == 0 {
			files++
			bytes += f.Size
		} else {
			deleted++
		}
	}
}

// GlobalSize returns the number of files, deleted files and total bytes for all
// files in the global model.
func (m *Model) GlobalSize() (files, deleted int, bytes int64) {
	m.fmut.RLock()
	fs := m.fs.Global()
	m.fmut.Unlock()
	return sizeOf(fs)
}

// LocalSize returns the number of files, deleted files and total bytes for all
// files in the local repository.
func (m *Model) LocalSize() (files, deleted int, bytes int64) {
	m.fmut.RLock()
	fs := m.fs.Have(0)
	m.fmut.Unlock()
	return sizeOf(fs)
}

// InSyncSize returns the number and total byte size of the local files that
// are in sync with the global model.
func (m *Model) InSyncSize() (files, bytes int64) {
	m.fmut.RLock()
	gf := m.fs.Global()
	hf := m.fs.Need(0)
	m.fmut.Unlock()

	gn, _, gb := sizeOf(gf)
	hn, _, hb := sizeOf(hf)

	return gn - hn, gb - hb
}

// NeedFiles returns the list of currently needed files and the total size.
func (m *Model) NeedFiles() ([]scanner.File, int64) {
	m.fmut.Lock()
	nf := m.fs.Need(0)
	m.fmut.Unlock()

	var bytes int64
	for _, n := range nf {
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
	cid := m.cm.Get(nodeID)
	m.fs.AddRemote(cid, files)
	m.fmut.Unlock()

	if debugNet {
		dlog.Printf("IDXUP(in): %s: %d files", nodeID, len(files))
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

	m.fq.RemoveAvailable(node)

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
	lf := m.fs.Get(0, name)
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

// HERERERERER

// ReplaceLocal replaces the local repository index with the given list of files.
func (m *Model) ReplaceLocal(fs []scanner.File) {
	var updated bool
	var newLocal = make(map[string]scanner.File)

	m.lmut.RLock()
	for _, f := range fs {
		newLocal[f.Name] = f
		if ef := m.local[f.Name]; !ef.Equals(f) {
			updated = true
		}
	}
	m.lmut.RUnlock()

	if m.markDeletedLocals(newLocal) {
		updated = true
	}

	m.lmut.RLock()
	if len(newLocal) != len(m.local) {
		updated = true
	}
	m.lmut.RUnlock()

	if updated {
		m.lmut.Lock()
		m.local = newLocal
		m.lmut.Unlock()

		m.recomputeGlobal()
		m.recomputeNeedForGlobal()

		m.umut.Lock()
		m.updatedLocal = time.Now().Unix()
		m.lastIdxBcastRequest = time.Now()
		m.umut.Unlock()
	}
}

// SeedLocal replaces the local repository index with the given list of files,
// in protocol data types. Does not track deletes, should only be used to seed
// the local index from a cache file at startup.
func (m *Model) SeedLocal(fs []protocol.FileInfo) {
	m.lmut.Lock()
	m.local = make(map[string]scanner.File)
	for _, f := range fs {
		m.local[f.Name] = fileFromFileInfo(f)
	}
	m.lmut.Unlock()

	m.recomputeGlobal()
	m.recomputeNeedForGlobal()
}

// Implements scanner.CurrentFiler
func (m *Model) CurrentFile(file string) scanner.File {
	m.lmut.RLock()
	f := m.local[file]
	m.lmut.RUnlock()
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

	m.initmut.Lock()
	rw := m.rwRunning
	m.initmut.Unlock()
	if !rw {
		return
	}

	for i := 0; i < m.parallelRequests; i++ {
		i := i
		go func() {
			if debugPull {
				dlog.Println("starting puller:", nodeID, i)
			}
			for {
				m.pmut.RLock()
				if _, ok := m.protoConn[nodeID]; !ok {
					if debugPull {
						dlog.Println("stopping puller:", nodeID, i)
					}
					m.pmut.RUnlock()
					return
				}
				m.pmut.RUnlock()

				qb, ok := m.fq.Get(nodeID)
				if ok {
					if debugPull {
						dlog.Println("request: out", nodeID, i, qb.name, qb.block.Offset)
					}
					data, _ := protoConn.Request("default", qb.name, qb.block.Offset, int(qb.block.Size))
					m.fq.Done(qb.name, qb.block.Offset, data)
				} else {
					time.Sleep(1 * time.Second)
				}
			}
		}()
	}
}

// ProtocolIndex returns the current local index in protocol data types.
// Must be called with the read lock held.
func (m *Model) ProtocolIndex() []protocol.FileInfo {
	var index []protocol.FileInfo

	m.lmut.RLock()

	for _, f := range m.local {
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

	m.lmut.RUnlock()
	return index
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
	for {
		m.umut.RLock()
		bcastRequested := m.lastIdxBcastRequest.After(m.lastIdxBcast)
		holdtimeExceeded := time.Since(m.lastIdxBcastRequest) > idxBcastHoldtime
		m.umut.RUnlock()

		maxDelayExceeded := time.Since(m.lastIdxBcast) > idxBcastMaxDelay
		if bcastRequested && (holdtimeExceeded || maxDelayExceeded) {
			idx := m.ProtocolIndex()

			var indexWg sync.WaitGroup
			indexWg.Add(len(m.protoConn))

			m.umut.Lock()
			m.lastIdxBcast = time.Now()
			m.umut.Unlock()

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
		time.Sleep(idxBcastHoldtime)
	}
}

// markDeletedLocals sets the deleted flag on files that have gone missing locally.
func (m *Model) markDeletedLocals(newLocal map[string]scanner.File) bool {
	// For every file in the existing local table, check if they are also
	// present in the new local table. If they are not, check that we already
	// had the newest version available according to the global table and if so
	// note the file as having been deleted.
	var updated bool

	m.gmut.RLock()
	m.lmut.RLock()

	for n, f := range m.local {
		if _, ok := newLocal[n]; !ok {
			if gf := m.global[n]; !gf.NewerThan(f) {
				if f.Flags&protocol.FlagDeleted == 0 {
					f.Flags = protocol.FlagDeleted
					f.Version++
					f.Blocks = nil
					updated = true
				}
				newLocal[n] = f
			}
		}
	}

	m.lmut.RUnlock()
	m.gmut.RUnlock()

	return updated
}

func (m *Model) updateLocal(f scanner.File) {
	var updated bool

	m.lmut.Lock()
	if ef, ok := m.local[f.Name]; !ok || !ef.Equals(f) {
		m.local[f.Name] = f
		updated = true
	}
	m.lmut.Unlock()

	if updated {
		m.recomputeGlobal()
		// We don't recomputeNeed here for two reasons:
		// - a need shouldn't have arisen due to having a newer local file
		// - recomputeNeed might call into fq.Add but we might have been called by
		//   fq which would be a deadlock on fq

		m.umut.Lock()
		m.updatedLocal = time.Now().Unix()
		m.lastIdxBcastRequest = time.Now()
		m.umut.Unlock()
	}
}

func (m *Model) recomputeGlobal() {
	var newGlobal = make(map[string]scanner.File)

	m.lmut.RLock()
	for n, f := range m.local {
		newGlobal[n] = f
	}
	m.lmut.RUnlock()

	var available = make(map[string][]string)

	m.rmut.RLock()
	var highestMod int64
	for nodeID, fs := range m.remote {
		for n, nf := range fs {
			if lf, ok := newGlobal[n]; !ok || nf.NewerThan(lf) {
				newGlobal[n] = nf
				available[n] = []string{nodeID}
				if nf.Modified > highestMod {
					highestMod = nf.Modified
				}
			} else if lf.Equals(nf) {
				available[n] = append(available[n], nodeID)
			}
		}
	}
	m.rmut.RUnlock()

	for f, ns := range available {
		m.fq.SetAvailable(f, ns)
	}

	// Figure out if anything actually changed

	m.gmut.RLock()
	var updated bool
	if highestMod > m.updateGlobal || len(newGlobal) != len(m.global) {
		updated = true
	} else {
		for n, f0 := range newGlobal {
			if f1, ok := m.global[n]; !ok || !f0.Equals(f1) {
				updated = true
				break
			}
		}
	}
	m.gmut.RUnlock()

	if updated {
		m.gmut.Lock()
		m.umut.Lock()
		m.global = newGlobal
		m.updateGlobal = time.Now().Unix()
		m.umut.Unlock()
		m.gmut.Unlock()
	}
}

type addOrder struct {
	n      string
	remote []scanner.Block
	fm     *fileMonitor
}

func (m *Model) recomputeNeedForGlobal() {
	var toDelete []scanner.File
	var toAdd []addOrder

	m.gmut.RLock()

	for _, gf := range m.global {
		toAdd, toDelete = m.recomputeNeedForFile(gf, toAdd, toDelete)
	}

	m.gmut.RUnlock()

	for _, ao := range toAdd {
		m.fq.Add(ao.n, ao.remote, ao.fm)
	}
	for _, gf := range toDelete {
		m.dq <- gf
	}
}

func (m *Model) recomputeNeedForFiles(files []scanner.File) {
	var toDelete []scanner.File
	var toAdd []addOrder

	m.gmut.RLock()

	for _, gf := range files {
		toAdd, toDelete = m.recomputeNeedForFile(gf, toAdd, toDelete)
	}

	m.gmut.RUnlock()

	for _, ao := range toAdd {
		m.fq.Add(ao.n, ao.remote, ao.fm)
	}
	for _, gf := range toDelete {
		m.dq <- gf
	}
}

func (m *Model) recomputeNeedForFile(gf scanner.File, toAdd []addOrder, toDelete []scanner.File) ([]addOrder, []scanner.File) {
	m.lmut.RLock()
	lf, ok := m.local[gf.Name]
	m.lmut.RUnlock()

	if !ok || gf.NewerThan(lf) {
		if gf.Suppressed {
			// Never attempt to sync invalid files
			return toAdd, toDelete
		}
		if gf.Flags&protocol.FlagDeleted != 0 && !m.delete {
			// Don't want to delete files, so forget this need
			return toAdd, toDelete
		}
		if gf.Flags&protocol.FlagDeleted != 0 && !ok {
			// Don't have the file, so don't need to delete it
			return toAdd, toDelete
		}
		if debugNeed {
			dlog.Printf("need: lf:%v gf:%v", lf, gf)
		}

		if gf.Flags&protocol.FlagDeleted != 0 {
			toDelete = append(toDelete, gf)
		} else {
			local, remote := scanner.BlockDiff(lf.Blocks, gf.Blocks)
			fm := fileMonitor{
				name:        FSNormalize(gf.Name),
				path:        FSNormalize(path.Clean(path.Join(m.dir, gf.Name))),
				global:      gf,
				model:       m,
				localBlocks: local,
			}
			toAdd = append(toAdd, addOrder{gf.Name, remote, &fm})
		}
	}

	return toAdd, toDelete
}

func (m *Model) WhoHas(name string) []string {
	var remote []string

	m.gmut.RLock()
	m.rmut.RLock()

	gf := m.global[name]
	for node, files := range m.remote {
		if file, ok := files[name]; ok && file.Equals(gf) {
			remote = append(remote, node)
		}
	}

	m.rmut.RUnlock()
	m.gmut.RUnlock()
	return remote
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
