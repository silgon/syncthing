package main

import (
	"compress/gzip"
	"crypto/sha1"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"path"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/calmh/ini"
	"github.com/calmh/syncthing/discover"
	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
)

const BlockSize = 128 * 1024

var cfg Configuration
var Version = "unknown-dev"

var (
	myID string
)

var (
	showVersion bool
	confDir     string
	verbose     bool
)

const (
	usage      = "syncthing [options]"
	extraUsage = `The following environemnt variables can be set to facilitate debugging:

 STPROFILER  Set to a listen address such as "127.0.0.1:9090" to start the
             profiler with HTTP access.

 STTRACE     A comma separated string of facilities to trace. The valid
             facility strings:
             - "scanner"  (the file change scanner)
             - "discover" (the node discovery package)
             - "net"      (connecting and disconnecting, sent/received messages)
             - "idx"      (index sending and receiving)
             - "need"     (file need calculations)
             - "pull"     (file pull activity)`
)

func main() {
	flag.StringVar(&confDir, "home", getDefaultConfDir(), "Set configuration directory")
	flag.BoolVar(&showVersion, "version", false, "Show version")
	flag.BoolVar(&verbose, "v", false, "Be more verbose")
	flag.Usage = usageFor(flag.CommandLine, usage, extraUsage)
	flag.Parse()

	if len(os.Getenv("STRESTART")) > 0 {
		// Give the parent process time to exit and release sockets etc.
		time.Sleep(1 * time.Second)
	}

	if showVersion {
		fmt.Println(Version)
		os.Exit(0)
	}

	if len(os.Getenv("GOGC")) == 0 {
		debug.SetGCPercent(25)
	}

	if len(os.Getenv("GOMAXPROCS")) == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	confDir = expandTilde(confDir)

	// Ensure that our home directory exists and that we have a certificate and key.

	ensureDir(confDir, 0700)
	cert, err := loadCert(confDir)
	if err != nil {
		newCertificate(confDir)
		cert, err = loadCert(confDir)
		fatalErr(err)
	}

	myID = string(certID(cert.Certificate[0]))
	log.SetPrefix("[" + myID[0:5] + "] ")
	logger.SetPrefix("[" + myID[0:5] + "] ")

	infoln("Version", Version)
	infoln("My ID:", myID)

	// Prepare to be able to save configuration

	cfgFile := path.Join(confDir, "config.xml")
	go saveConfigLoop(cfgFile)

	// Load the configuration file, if it exists.
	// If it does not, create a template.

	cf, err := os.Open(cfgFile)
	if err == nil {
		// Read config.xml
		cfg, err = readConfigXML(cf)
		if err != nil {
			fatalln(err)
		}
		cf.Close()
	} else {
		// No config.xml, let's try the old syncthing.ini
		iniFile := path.Join(confDir, "syncthing.ini")
		cf, err := os.Open(iniFile)
		if err == nil {
			infoln("Migrating syncthing.ini to config.xml")
			iniCfg := ini.Parse(cf)
			cf.Close()
			os.Rename(iniFile, path.Join(confDir, "migrated_syncthing.ini"))

			cfg, _ = readConfigXML(nil)
			cfg.Repositories = []RepositoryConfiguration{
				{Directory: iniCfg.Get("repository", "dir")},
			}
			readConfigINI(iniCfg.OptionMap("settings"), &cfg.Options)
			for name, addrs := range iniCfg.OptionMap("nodes") {
				n := NodeConfiguration{
					NodeID:    name,
					Addresses: strings.Fields(addrs),
				}
				cfg.Repositories[0].Nodes = append(cfg.Repositories[0].Nodes, n)
			}

			saveConfig()
		}
	}

	if len(cfg.Repositories) == 0 {
		infoln("No config file; starting with empty defaults")

		cfg, err = readConfigXML(nil)
		cfg.Repositories = []RepositoryConfiguration{
			{
				ID:        "default",
				Directory: path.Join(getHomeDir(), "Sync"),
				Nodes: []NodeConfiguration{
					{NodeID: myID, Addresses: []string{"dynamic"}},
				},
			},
		}

		saveConfig()
		infof("Edit %s to taste or use the GUI\n", cfgFile)
	}

	var m = newModel()
	if cfg.Options.MaxSendKbps > 0 {
		m.LimitSendRate(cfg.Options.MaxSendKbps)
	}

	for i, repo := range cfg.Repositories {
		// Make sure the local node is in the node list.
		cfg.Repositories[i].Nodes = cleanNodeList(cfg.Repositories[i].Nodes, myID)

		var dir = expandTilde(repo.Directory)
		ensureDir(dir, -1)

		var nodes []string
		for _, node := range repo.Nodes {
			nodes = append(nodes, node.NodeID)
		}

		m.AddRepository(repo.ID, dir, nodes)
	}

	if profiler := os.Getenv("STPROFILER"); len(profiler) > 0 {
		go func() {
			dlog.Println("Starting profiler on", profiler)
			err := http.ListenAndServe(profiler, nil)
			if err != nil {
				dlog.Fatal(err)
			}
		}()
	}

	// The TLS configuration is used for both the listening socket and outgoing
	// connections.

	tlsCfg := &tls.Config{
		Certificates:           []tls.Certificate{cert},
		NextProtos:             []string{"bep/1.0"},
		ServerName:             myID,
		ClientAuth:             tls.RequestClientCert,
		SessionTicketsDisabled: true,
		InsecureSkipVerify:     true,
		MinVersion:             tls.VersionTLS12,
	}

	// GUI
	if cfg.Options.GUIEnabled && cfg.Options.GUIAddress != "" {
		addr, err := net.ResolveTCPAddr("tcp", cfg.Options.GUIAddress)
		if err != nil {
			warnf("Cannot start GUI on %q: %v", cfg.Options.GUIAddress, err)
		} else {
			var hostOpen, hostShow string
			switch {
			case addr.IP == nil:
				hostOpen = "localhost"
				hostShow = "0.0.0.0"
			case addr.IP.IsUnspecified():
				hostOpen = "localhost"
				hostShow = addr.IP.String()
			default:
				hostOpen = addr.IP.String()
				hostShow = hostOpen
			}

			infof("Starting web GUI on http://%s:%d/", hostShow, addr.Port)
			startGUI(cfg.Options.GUIAddress, m)
			if cfg.Options.StartBrowser && len(os.Getenv("STRESTART")) == 0 {
				openURL(fmt.Sprintf("http://%s:%d", hostOpen, addr.Port))
			}
		}
	}

	// Load indexes and rehash local files
	updateLocalModel(m)

	connOpts := map[string]string{
		"clientId":      "syncthing",
		"clientVersion": Version,
		//"clusterHash":   clusterHash(cfg.Repositories[0].Nodes),
	}

	// Routine to listen for incoming connections
	if verbose {
		infoln("Listening for incoming connections")
	}
	for _, addr := range cfg.Options.ListenAddress {
		go listen(myID, addr, m, tlsCfg, connOpts)
	}

	// Routine to connect out to configured nodes
	if verbose {
		infoln("Attempting to connect to other nodes")
	}
	disc := discovery(cfg.Options.ListenAddress[0])
	go connect(myID, disc, m, tlsCfg, connOpts)

	// Routine to pull blocks from other nodes to synchronize the local
	// repository. Does not run when we are in read only (publish only) mode.
	if !cfg.Options.ReadOnly {
		if verbose {
			if cfg.Options.AllowDelete {
				infoln("Deletes from peer nodes are allowed")
			} else {
				infoln("Deletes from peer nodes will be ignored")
			}
			okln("Ready to synchronize (read-write)")
		}
		m.StartRW(cfg.Options.AllowDelete, cfg.Options.ParallelRequests)
	} else if verbose {
		okln("Ready to synchronize (read only; no external updates accepted)")
	}

	select {}
}

func restart() {
	infoln("Restarting")
	env := os.Environ()
	if len(os.Getenv("STRESTART")) == 0 {
		env = append(env, "STRESTART=1")
	}
	pgm, err := exec.LookPath(os.Args[0])
	if err != nil {
		warnln(err)
		return
	}
	proc, err := os.StartProcess(pgm, os.Args, &os.ProcAttr{
		Env:   env,
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
	})
	if err != nil {
		fatalln(err)
	}
	proc.Release()
	os.Exit(0)
}

var saveConfigCh = make(chan struct{})

func saveConfigLoop(cfgFile string) {
	for _ = range saveConfigCh {
		fd, err := os.Create(cfgFile + ".tmp")
		if err != nil {
			warnln(err)
			continue
		}

		err = writeConfigXML(fd, cfg)
		if err != nil {
			warnln(err)
			fd.Close()
			continue
		}

		err = fd.Close()
		if err != nil {
			warnln(err)
			continue
		}

		if runtime.GOOS == "windows" {
			err := os.Remove(cfgFile)
			if err != nil && !os.IsNotExist(err) {
				warnln(err)
			}
		}

		err = os.Rename(cfgFile+".tmp", cfgFile)
		if err != nil {
			warnln(err)
		}
	}
}

func saveConfig() {
	saveConfigCh <- struct{}{}
}

func listen(myID string, addr string, m *model, tlsCfg *tls.Config, connOpts map[string]string) {
	if debugNet {
		dlog.Println("listening on", addr)
	}
	l, err := tls.Listen("tcp", addr, tlsCfg)
	fatalErr(err)

	for {
		conn, err := l.Accept()
		if err != nil {
			warnln(err)
			continue
		}

		if debugNet {
			dlog.Println("connect from", conn.RemoteAddr())
		}

		tc := conn.(*tls.Conn)
		err = tc.Handshake()
		if err != nil {
			warnln(err)
			tc.Close()
			continue
		}

		remoteID := certID(tc.ConnectionState().PeerCertificates[0].Raw)

		if remoteID == myID {
			warnf("Connect from myself (%s) - should not happen", remoteID)
			conn.Close()
			continue
		}

		if m.GetConnection(remoteID) != nil {
			warnf("Connect from connected node (%s)", remoteID)
		}

		protoConn := protocol.NewConnection(remoteID, conn, conn, conn, connectionDelegate{m})
		m.AddConnection(protoConn)
	}
}

func discovery(addr string) *discover.Discoverer {
	_, portstr, err := net.SplitHostPort(addr)
	fatalErr(err)
	port, _ := strconv.Atoi(portstr)

	if !cfg.Options.LocalAnnEnabled {
		port = -1
	} else if verbose {
		infoln("Sending local discovery announcements")
	}

	if !cfg.Options.GlobalAnnEnabled {
		cfg.Options.GlobalAnnServer = ""
	} else if verbose {
		infoln("Sending external discovery announcements")
	}

	disc, err := discover.NewDiscoverer(myID, port, cfg.Options.GlobalAnnServer)

	if err != nil {
		warnf("No discovery possible (%v)", err)
	}

	return disc
}

// allNodeAddrs returns a flattened list of (node, addr) pairs
func allNodeAddrs() [][2]string {
	var addrs [][2]string
	for _, repo := range cfg.Repositories {
		for _, cfg := range repo.Nodes {
			for _, addr := range cfg.Addresses {
				addrs = append(addrs, [2]string{cfg.NodeID, addr})
			}
		}
	}
	return addrs
}

func connect(myID string, disc *discover.Discoverer, m *model, tlsCfg *tls.Config, connOpts map[string]string) {
	for {
		var connAddrs [][2]string

		// Filter out addresses for nodes we are not connected to, and attempt
		// to resolve the dynamic addresses.

		for _, addrSpec := range allNodeAddrs() {
			node, addr := addrSpec[0], addrSpec[1]

			if node == myID {
				continue
			}

			if m.GetConnection(node) != nil {
				continue
			}

			if addr == "dynamic" {
				if disc != nil {
					for _, discAddr := range disc.Lookup(node) {
						connAddrs = append(connAddrs, [2]string{node, discAddr})
						if debugNet {
							dlog.Println("discovered", discAddr, "for", node)
						}
					}
				}
			} else {
				connAddrs = append(connAddrs, addrSpec)
			}
		}

		// Attempt to connect to the addresses remaining after filtering and
		// resolving.

		for _, addrSpec := range connAddrs {
			node, addr := addrSpec[0], addrSpec[1]

			if m.GetConnection(node) != nil {
				continue
			}

			if debugNet {
				dlog.Println("dial", node, addr)
			}

			conn, err := tls.Dial("tcp", addr, tlsCfg)
			if err != nil {
				if debugNet {
					dlog.Println(err)
				}
				continue
			}

			remoteID := certID(conn.ConnectionState().PeerCertificates[0].Raw)
			if remoteID != node {
				warnln("Unexpected nodeID", remoteID, "!=", node)
				conn.Close()
				continue
			}

			protoConn := protocol.NewConnection(remoteID, conn, conn, conn, connectionDelegate{m})
			m.AddConnection(protoConn)
		}

		time.Sleep(time.Duration(cfg.Options.ReconnectIntervalS) * time.Second)
	}
}

func updateLocalModel(m *model) {
	for _, rd := range m.RepoDirs() {
		repo, dir := rd[0], rd[1]

		if verbose {
			infoln("Loading cache for repo", repo)
		}
		cacheIdx := loadIndex(repo, dir)
		m.InitialRepoContents(repo, protocolToScannerSlice(cacheIdx))

		sup := &suppressor{threshold: int64(cfg.Options.MaxChangeKbps)}
		w := &scanner.Walker{
			Dir:            dir,
			IgnoreFile:     ".stignore",
			FollowSymlinks: cfg.Options.FollowSymlinks,
			BlockSize:      BlockSize,
			Suppressor:     sup,
			TempNamer:      defTempNamer,
		}

		if verbose {
			infoln("Refreshing repo", repo)
		}
		files, _ := w.Walk()
		m.UpdateRepoContents(repo, files)

		go func() {
			sl := time.Duration(cfg.Options.RescanIntervalS) * time.Second
			for {
				files, _ := w.Walk()
				m.UpdateRepoContents(repo, files)
				saveIndex(repo, dir, files) // TODO: only if it changed
				time.Sleep(sl)
			}
		}()
	}
}

func idxName(repo, dir string) string {
	return fmt.Sprintf("%x.idx.gz", sha1.Sum([]byte(repo+"|"+dir)))
}

func saveIndex(repo, dir string, files []scanner.File) {
	name := idxName(repo, dir)
	fullName := path.Join(confDir, name)
	idxf, err := os.Create(fullName + ".tmp")
	if err != nil {
		return
	}

	gzw := gzip.NewWriter(idxf)

	protocol.IndexMessage{
		Repository: repo,
		Files:      scannerToProtocolSlice(files),
	}.EncodeXDR(gzw)
	gzw.Close()
	idxf.Close()
	os.Rename(fullName+".tmp", fullName)
}

func loadIndex(repo, dir string) []protocol.FileInfo {
	name := idxName(repo, dir)
	idxf, err := os.Open(path.Join(confDir, name))
	if err != nil {
		return nil
	}
	defer idxf.Close()

	gzr, err := gzip.NewReader(idxf)
	if err != nil {
		return nil
	}
	defer gzr.Close()

	var im protocol.IndexMessage
	err = im.DecodeXDR(gzr)
	if err != nil || im.Repository != repo {
		return nil
	}
	return im.Files
}

func ensureDir(dir string, mode int) {
	fi, err := os.Stat(dir)
	if os.IsNotExist(err) {
		err := os.MkdirAll(dir, 0700)
		fatalErr(err)
	} else if mode >= 0 && err == nil && int(fi.Mode()&0777) != mode {
		err := os.Chmod(dir, os.FileMode(mode))
		fatalErr(err)
	}
}

func expandTilde(p string) string {
	if runtime.GOOS == "windows" {
		return p
	}

	if strings.HasPrefix(p, "~/") {
		return strings.Replace(p, "~", getUnixHomeDir(), 1)
	}
	return p
}

func getUnixHomeDir() string {
	home := os.Getenv("HOME")
	if home == "" {
		fatalln("No home directory?")
	}
	return home
}

func getHomeDir() string {
	if runtime.GOOS == "windows" {
		home := os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		return home
	}
	return getUnixHomeDir()
}

func getDefaultConfDir() string {
	if runtime.GOOS == "windows" {
		return path.Join(os.Getenv("AppData"), "syncthing")
	}
	return expandTilde("~/.syncthing")
}
