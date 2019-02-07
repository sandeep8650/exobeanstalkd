package priorityqueue
import (
	"container/heap"
	"exotel/exobeanstalkd/types"
	"errors"
)
type container []*types.Job
// PriorityQueue represents the queue
type PriorityQueue struct {
	jobHeap *container
	//lookup   map[interface{}](*types.Job)
	//lookup map ID->index
	lookup map[int]int
}

// New initializes an empty priority queue.
func New() PriorityQueue {
	return PriorityQueue{
		jobHeap: &container{},
		lookup:   make(map[int]int),
	}
}

// Len returns the number of elements in the queue.
func (p *PriorityQueue) Len() int {
	return p.jobHeap.Len()
}

// Insert inserts a new element into the queue. No action is performed on duplicate elements.
func (p *PriorityQueue) Insert(jb *types.Job) {
	_, ok := p.lookup[jb.ID]
	if ok {
		return
	}

	heap.Push(p.jobHeap, jb)
	p.lookup[jb.ID] = jb.Index
}

// Pop removes the element with the highest priority from the queue and returns it.
// In case of an empty queue, an error is returned.
func (p *PriorityQueue) Pop() (*types.Job, error) {
	if len(*p.jobHeap) == 0 {
		return nil, errors.New("empty queue")
	}

	job := heap.Pop(p.jobHeap).(*types.Job)
	delete(p.lookup, job.ID)
	return job, nil
}

// UpdatePriority changes the priority of a given item.
// If the specified item is not present in the queue, no action is performed.
/*func (p *PriorityQueue) UpdatePriority(x interface{}, newPriority float64) {
	item, ok := p.lookup[x]
	if !ok {
		return
	}

	item.priority = newPriority
	heap.Fix(p.itemHeap, item.index)
}*/

func (c *container) Len() int {
	return len(*c)
}

func (c *container) Less(i, j int) bool {
	return (*c)[i].ID < (*c)[j].ID
}

func (c *container) Swap(i, j int) {
	(*c)[i], (*c)[j] = (*c)[j], (*c)[i]
	(*c)[i].index = i
	(*c)[j].index = j
}

func (c *container) Push(x interface{}) {
	it := x.(*item)
	it.index = len(*ih)
	*ih = append(*ih, it)
}

func (c *container) Pop() interface{} {
	old := *ih
	item := old[len(old)-1]
	*ih = old[0 : len(old)-1]
	return item
}

import "container/heap"
import "exotel/exobeanstalkd/types"
