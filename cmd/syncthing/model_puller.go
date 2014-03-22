//+build ignore

package main

import (
	"os"
	"path"

	"github.com/calmh/syncthing/buffers"
)

type requestResult struct {
	node   string
	repo   string
	file   string // repo-relative name
	path   string // full path name, fs-normalized
	offset int64
	data   []byte
	err    error
}

type openFile struct {
	path         string // full path name, fs-normalized
	temp         string // temporary filename, full path, fs-normalized
	availability uint64 // availability bitset
	file         *os.File
	err          error // error when opening or writing to file, all following operations are cancelled
	outstanding  int   // number of requests we still have outstanding
	done         bool  // we have sent all requests for this file
}

const slots = 8

var requestResults chan requestResult
var openFiles map[string]openFile
var bq = newBlockQueue()
var requestQueue = make(chan queuedBlock, slots)
var oustandingPerNode = make(map[string]int)

var requestSlots = make(chan bool, slots)
var blocks = make(chan bqBlock)

func (m *Model) leastBusyNode(availability uint64) {
	var low int = 2<<63 - 1
	var selected string
	for node, usage := range oustandingPerNode {
		if availability&1<<m.cm.Get(node) != 0 {
			if usage < low {
				low = usage
				selected = node
			}
		}
	}
	oustandingPerNode[selected]++
	return selected
}

func (m *Model) puller(repo string, dir string) {
	for i := 0; i < slots; i++ {
		requestSlots <- true
	}
	go func() {
		// fill blocks queue when there are free slots
		for {
			<-requestSlots
			blocks <- <-bq.outbox
		}
	}()

pull:
	for {
		select {
		case res := <-requestResults:
			oustandingPerNode[res.node]--
			of, ok := openFiles[res.file]
			if !ok || of.err != nil {
				// no entry in openFiles means there was an error and we've cancelled the operation
				continue
			}
			of.err = of.file.WriteAt(res.data, res.offset)
			buffers.Put(res.data)
			of.outstanding--
			if of.done && of.outstanding == 0 {
				of.file.Close()
				delete(openFiles, res.file)
				// Hash check the file and rename
			}
			requestSlots <- true

		case b := <-blocks:
			f := b.file

			of, ok := openFiles[f.Name]
			if !ok {
				of.path = FSNormalize(path.Join(dir, f.Name))
				of.temp = FSNormalize(path.Join(dir, defTempNamer.TempName(f.Name)))
				of.availability = m.fs[repo].Availability(f.Name)
				of.done = b.last

				of.file, of.err = os.OpenFile(of.temp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, osFileMode(f.Flags&0777))
				if of.err != nil {
					openFiles[f.Name] = of
					continue pull
				}
			}

			if len(b.copy) > 0 {
				// We have blocks to copy from the existing file
				exfd, of.err = os.Open(of.path)
				if of.err != nil {
					of.file.Close()
					openFiles[f.Name] = of
					continue pull
				}

				for _, b := range b.copy {
					bs := buffers.Get(b.Size)
					of.err = exfd.ReadAt(bs, b.Offset)
					if of.err == nil {
						of.err = of.file.WriteAt(bs, b.Offset)
					}
					buffers.Put(bs)
					if of.err != nil {
						exfd.Close()
						of.file.Close()
						openFiles[f.Name] = of
						continue pull
					}
				}

				exfd.Close()
			}

			if of.block.Size > 0 {
				openFiles[b.Name].outstanding++
				// TODO: Select a peer
				go func(node string, b queuedBlock) {
					bs, err := m.protoConn[node].Request("default", f.name, b.offset, b.size)
					requestResults <- requestResult{b.name, b.offset, bs, err}
					requestSlots <- true
				}(node, b)
			} else {
				// nothing more to do
				requestSlots <- true
			}
		}
	}
}
