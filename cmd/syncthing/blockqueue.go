package main

import "sync"

type queuedBlock struct {
	file   string
	offset int64
	size   uint32
	last   bool
}

type blockQueue struct {
	blocks []queuedBlock
	mutex  sync.Mutex
	empty  *sync.Cond
}

func newBlockQueue() *blockQueue {
	q := &blockQueue{}
	q.empty = sync.NewCond(&q.mutex)
	return q
}

func (q *blockQueue) contains(f string) bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	for _, b := range q.blocks {
		if b.file == f {
			return true
		}
	}
	return false
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
