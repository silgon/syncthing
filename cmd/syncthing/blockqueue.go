package main

import "github.com/calmh/syncthing/scanner"

type bqAdd struct {
	file scanner.File
	have []scanner.Block
	need []scanner.Block
}

type bqBlock struct {
	file  scanner.File
	block scanner.Block   // get this block from the network
	copy  []scanner.Block // copy these blocks from the old version of the file
}

type blockQueue struct {
	inbox  chan bqAdd
	outbox chan bqBlock

	queued []bqBlock
}

func newBlockQueue() *blockQueue {
	q := &blockQueue{
		inbox:  make(chan bqAdd),
		outbox: make(chan bqBlock),
	}
	go q.run()
	return q
}

func (q *blockQueue) addBlock(a bqAdd) {
	// First queue a copy operation
	q.queued = append(q.queued, bqBlock{
		file: a.file,
		copy: a.have,
	})
	// Then queue the needed blocks individually
	for _, b := range a.need {
		q.queued = append(q.queued, bqBlock{
			file:  a.file,
			block: b,
		})
	}
}

func (q *blockQueue) run() {
	for {
		if len(q.queued) == 0 {
			q.addBlock(<-q.inbox)
		} else {
			next := q.queued[0]
			select {
			case a := <-q.inbox:
				q.addBlock(a)
			case q.outbox <- next:
				q.queued = q.queued[1:]
			}
		}
	}
}

func (q *blockQueue) put(a bqAdd) {
	q.inbox <- a
}
