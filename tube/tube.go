/*
 **********************
 * Sandeep Singh
 **********************
 */

package tube

import (
	"container/heap"
	"errors"
	"exotel.in/exobeanstalkd/types"
	"fmt"
)

var id = -1

func getID() int {
	id++
	return id
}

type container struct {
	itemHeap []*types.Job
	//lookupIndex   map[interface{}](*types.Job)
	//lookupIndex map ID->index
	lookupIndex map[int]int
}

// Tube represents the queue
type Tube struct {
	Name    string
	ID      int
	jobHeap *container
}

// New initializes an empty tube.
func New(name string) Tube {
	return Tube{
		Name:    name,
		ID:      getID(),
		jobHeap: &container{lookupIndex: make(map[int]int)},
	}
}

// Len returns the number of elements in the queue.
func (tb *Tube) Len() int {
	return tb.jobHeap.Len()
}

// Push inserts a new element into the queue. No action is performed on duplicate elements.
func (tb *Tube) Push(jb *types.Job) {
	_, ok := tb.jobHeap.lookupIndex[jb.ID]
	if ok {
		return
	}

	heap.Push(tb.jobHeap, jb)
	//tb.lookupIndex[jb.ID] = jb.Index //Don't need it bcuz Push will take care of this
}

// Pop removes the element with the highest priority from the queue and returns it.
// In case of an empty queue, an error is returned.
func (tb *Tube) Pop() (*types.Job, error) {
	if tb.Len() == 0 {
		return nil, errors.New("empty tube")
	}

	job := heap.Pop(tb.jobHeap).(*types.Job)
	//delete(tb.jobHeap.lookupIndex, job.ID)
	return job, nil
}

//Top returns priority of next ready job
func (tb *Tube) Top() (int, error) {
	if tb.Len() == 0 {
		return 0, errors.New("empty tube")
	}
	priority := tb.jobHeap.itemHeap[0].Priority
	return priority, nil
}

//Delete deletes job from tube with job id ID
func (tb *Tube) Delete(ID int) (*types.Job, error) {
	indx, ok := tb.jobHeap.lookupIndex[ID]
	if !ok {
		err := fmt.Sprintf("No job with ID %d in tube %s", ID, (*tb).Name)
		return nil, errors.New(err)
	}
	job := heap.Remove(tb.jobHeap, indx).(*types.Job)
	//delete(tb.jobHeap.lookupIndex, job.ID)
	return job, nil
}

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
	job := x.(*types.Job)
	job.Index = c.Len()
	(*c).itemHeap = append((*c).itemHeap, job)
	(*c).lookupIndex[job.ID] = job.Index
}

func (c *container) Pop() interface{} {
	old := (*c).itemHeap
	job := old[len(old)-1]
	(*c).itemHeap = old[0 : len(old)-1]
	delete((*c).lookupIndex, job.ID)
	return job
}
