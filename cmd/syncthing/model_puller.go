//+build ignore

package main

import (
	"os"
	"sync"
)

type queuedBlock struct {
	file   string
	offset int64
	size   int32
	last   bool
}

type blockQueue struct {
	blocks []queuedBlock
	mutex  sync.Mutex
	empty  sync.Cond
}

func newBlockQueue() *blockQueue {
	q := &blockQueue{}
	q.empty = sync.NewCond(q.mutex)
	return q
}

func (q *blockQueue) put(b queuedBlock) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.blocks = append(q.blocks, b)
	q.empty.Signal()
}

func (q *blockQueue) get() queuedBlock {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	for len(q.blocks) == 0 {
		q.empty.Wait()
	}

	b := q.blocks[0]
	q.blocks = q.blocks[1:]

	return b
}

type reqRes struct {
	node   string
	repo   string
	file   string
	offset int64
	data   []byte
	err    error
}

type filestatus struct {
	file        *os.File
	outstanding int  // Number of Requests we still have outstanding
	done        bool // We have sent all Requests for this file
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
				// Open and copy file
			}
			fs.outstanding++
			fs.done = b.last

			availability := f.fs.Availability(b.Name)
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
