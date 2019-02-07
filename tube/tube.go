/*
 **********************
 * Sandeep Singh
 **********************
 */

package tube

import pq "exotel/exobeanstalkd/priorityqueue"
import "exotel/exobeanstalkd/types"

var id = -1

//Tube data type
//Name: name of tube
//id: id of tube
//pq: priorityqueue to store job
type Tube struct {
	Name string
	id   int
	pQ   pq.PriorityQueue
}

func getID() int {
	id++
	return id
}

//New creates and return new tube
func New(name string) Tube {
	return Tube{
		Name: name,
		id:   getID(),
		pQ:   pq.New(),
	}
}

//Insert insertes job in tube
func (tb *Tube) Insert(job *types.Job) {
	(*tb).pQ.Push(job)
}
