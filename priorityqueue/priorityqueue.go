package priorityqueue

import (
	"container/heap"
	"errors"
	"exotel/exobeanstalkd/types"
)

type container struct {
	itemHeap []*types.Job
	//lookupIndex   map[interface{}](*types.Job)
	//lookupIndex map ID->index
	lookupIndex map[int]int
}

// PriorityQueue represents the queue
type PriorityQueue struct {
	jobHeap *container
}

// New initializes an empty priority queue.
func New() PriorityQueue {
	return PriorityQueue{
		jobHeap: &container{lookupIndex: make(map[int]int)},
	}
}

// Len returns the number of elements in the queue.
func (p *PriorityQueue) Len() int {
	return p.jobHeap.Len()
}

// Push inserts a new element into the queue. No action is performed on duplicate elements.
func (p *PriorityQueue) Push(jb *types.Job) {
	_, ok := p.jobHeap.lookupIndex[jb.ID]
	if ok {
		return
	}

	heap.Push(p.jobHeap, jb)
	//p.lookupIndex[jb.ID] = jb.Index //Don't need it bcuz Push will take care of this
}

// Pop removes the element with the highest priority from the queue and returns it.
// In case of an empty queue, an error is returned.
func (p *PriorityQueue) Pop() (*types.Job, error) {
	if p.Len() == 0 {
		return nil, errors.New("empty queue")
	}

	job := heap.Pop(p.jobHeap).(*types.Job)
	delete(p.jobHeap.lookupIndex, job.ID)
	return job, nil
}

//Remove deletes job from queue with job id ID
func (p *PriorityQueue) Remove(ID int) bool {
	indx, ok := p.jobHeap.lookupIndex[ID]
	if !ok {
		return false
	}
	_ = heap.Remove(p.jobHeap, indx).(*types.Job)
	return true
}

// UpdatePriority changes the priority of a given item.
// If the specified item is not present in the queue, no action is performed.
/*func (p *PriorityQueue) UpdatePriority(x interface{}, newPriority float64) {
	item, ok := p.lookupIndex[x]
	if !ok {
		return
	}

	item.priority = newPriority
	heap.Fix(p.itemHeap, item.index)
}*/

func (c *container) Len() int {
	return len((*c).itemHeap)
}

func (c *container) Less(i, j int) bool {
	return (*c).itemHeap[i].ID < (*c).itemHeap[j].ID
}

func (c *container) Swap(i, j int) {
	(*c).itemHeap[i], (*c).itemHeap[j] = (*c).itemHeap[j], (*c).itemHeap[i]
	(*c).itemHeap[i].Index = i
	(*c).itemHeap[j].Index = j
	(*c).lookupIndex[(*c).itemHeap[i].ID] = i
	(*c).lookupIndex[(*c).itemHeap[j].ID] = j
}

func (c *container) Push(x interface{}) {
	jb := x.(*types.Job)
	jb.Index = c.Len()
	(*c).itemHeap = append((*c).itemHeap, jb)
	(*c).lookupIndex[jb.ID] = jb.Index
}

func (c *container) Pop() interface{} {
	old := (*c).itemHeap
	job := old[len(old)-1]
	(*c).itemHeap = old[0 : len(old)-1]
	return job
}
