//+build ignore

package main

import "os"

type reqRes struct {
	node   string
	repo   string
	file   string
	offset int64
	data   []byte
	err    error
}

type filestatus struct {
	file         *os.File
	err          error
	outstanding  int  // number of requests we still have outstanding
	done         bool // we have sent all requests for this file
	availability uint64
	temp         string // temporary filename
}

const slots = 8

var requestResults chan reqRes
var files map[string]filestatus
var bq = newBlockQueue()
var requestQueue = make(chan queuedBlock, slots)
var requestSlots = make(chan bool, slots)
var oustandingPerNode = make(map[string]int)

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

func (m *Model) puller() {
	for {
		select {
		case res := <-requestResults:
			oustandingPerNode[res.node]--
			fs := files[res.file]
			fs.file.WriteAt(res.data, res.offset)
			fs.outstanding--
			if fs.done && fs.outstanding == 0 {
				fs.file.Close()
				// Hash check the file and rename
			}
			requestSlots <- true

		case b := <-requestQueue:
			if fs, ok := files[b.Name]; !ok {
				fs.temp = defTempNamer.TempName(b.Name)
				fs.file, fs.err := os.Create(fs.temp)
				// Open and copy file
				fs.availability = m.fs.Availability(b.Name)
			}
			fs.outstanding++
			fs.done = b.last
			files[b.Name] = fs

			// Select a peer
			go func(node string, b queuedBlock) {
				bs, err := m.protoConn[node].Request("default", b.name, b.offset, b.size)
				requestResults <- reqRes{b.name, b.offset, bs, err}
			}(node, b)
		}
	}
}

func (m *Model) filler() {
	// Populate requestSlots with the number of requests we can process in parallel
	for i := 0; i < slots; i++ {
		requestSlots <- true
	}

	for {
		// Whenever there is a free slot, get a new block from the queue and put it on to the blockQueue.
		select {
		case <-requestSlots:
			requestQueue <- bq.get()
		}
	}
}
