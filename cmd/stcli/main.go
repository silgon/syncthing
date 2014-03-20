package main

import (
	"crypto/tls"
	"flag"
	"io"
	"log"
	"os"

	"github.com/calmh/syncthing/protocol"
)

var (
	exit    bool
	cmd     string
	confDir string
	target  string
)

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)

	flag.StringVar(&cmd, "cmd", "idx", "Command")
	flag.StringVar(&confDir, "home", ".", "Certificates directory")
	flag.StringVar(&target, "target", "127.0.0.1:22000", "Target node")
	flag.BoolVar(&exit, "exit", false, "Exit after command")
	flag.Parse()

	connect(target)

	select {}
}

func connect(target string) {
	cert, err := loadCert(confDir)
	if err != nil {
		log.Fatal(err)
	}

	myID := string(certID(cert.Certificate[0]))

	tlsCfg := &tls.Config{
		Certificates:           []tls.Certificate{cert},
		NextProtos:             []string{"bep/1.0"},
		ServerName:             myID,
		ClientAuth:             tls.RequestClientCert,
		SessionTicketsDisabled: true,
		InsecureSkipVerify:     true,
		MinVersion:             tls.VersionTLS12,
	}

	conn, err := tls.Dial("tcp", target, tlsCfg)
	if err != nil {
		log.Fatal(err)
	}

	remoteID := certID(conn.ConnectionState().PeerCertificates[0].Raw)

	protocol.NewConnection(remoteID, conn, conn, Model{}, nil)

	select {}
}

type Model struct {
}

func prtIndex(files []protocol.FileInfo) {
	for _, f := range files {
		log.Printf("%q (v:%d mod:%d flag:0x%x)", f.Name, f.Version, f.Modified, f.Flags)
		for _, b := range f.Blocks {
			log.Printf("    %6d %x", b.Size, b.Hash)
		}
	}
}

func (m Model) Index(nodeID string, files []protocol.FileInfo) {
	log.Printf("Received index")
	if cmd == "idx" {
		prtIndex(files)
		if exit {
			os.Exit(0)
		}
	}
}

func (m Model) IndexUpdate(nodeID string, files []protocol.FileInfo) {
	log.Println("Received index update")
	if cmd == "idx" {
		prtIndex(files)
		if exit {
			os.Exit(0)
		}
	}
}

func (m Model) Request(nodeID, repo string, name string, offset int64, size int) ([]byte, error) {
	log.Println("Received request")
	return nil, io.EOF
}

func (m Model) Close(nodeID string, err error) {
	log.Println("Received close")
}
