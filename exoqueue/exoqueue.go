/*
 **********************
 * Sandeep Singh
 **********************
 */

package exoqueue

import (
	"errors"
	"exotel/exobeanstalkd/tube"
	"exotel/exobeanstalkd/types"
	"fmt"
	"sync"
	"time"
)

type foo struct {
	tubeID int
	job    *types.Job
}

//Queue struct
//tubes: slice of pointers to Tube struct
//currentTube: ID of tube currently in use
//watchList: list of tube ids which are in watchlist
//jobIDtoTubeID: mapping from jobID to tubeID
//tubeNameToTubeID: mapping from tubeName to tubeID
//nextJobID:
//reserveTube: mapping from jobID to struct{jobId,tubeID}
type Queue struct {
	tubes            []*tube.Tube
	currentTube      int
	watchList        map[int]struct{}
	jobIDtoTubeID    map[int]int
	tubeNameToTubeID map[string]int
	nextJobID        int
	reserveTube      map[int]foo
	mux              sync.Mutex
}

//New returns empty queue with default tube
func New() Queue {
	defaultTube := tube.New("default")
	map1 := make(map[string]int)
	map1["default"] = 0

	map2 := make(map[int]struct{})
	map2[0] = struct{}{}
	return Queue{
		tubes:            []*tube.Tube{&(defaultTube)},
		currentTube:      0, //ID of tube in use
		watchList:        map2,
		jobIDtoTubeID:    make(map[int]int),
		tubeNameToTubeID: map1,
		nextJobID:        -1,
		reserveTube:      make(map[int]foo),
	}
}

//Use selects current tube if tube exist and if not then create new one
func (q *Queue) Use(tbName string) string {
	tbID, ok := q.tubeNameToTubeID[tbName]
	if ok {
		q.currentTube = tbID
	} else {
		n := len(q.tubes) //ID of new tube
		newTube := tube.New(tbName)
		q.tubes = append(q.tubes, &newTube)
		q.tubeNameToTubeID[tbName] = n
		q.currentTube = n
	}
	return tbName
}

func (q *Queue) getNextJobID() int {
	q.nextJobID++
	return q.nextJobID
}

//Put inserts the job in queue and returns id of inserted job
func (q *Queue) Put(priority int, ttr int, count int, data []byte) int {
	if ttr <= 0 {
		ttr = 1
	}
	job := types.Job{
		ID:       q.getNextJobID(),
		Priority: priority,
		TTR:      ttr,
		Data:     data,
	}
	q.jobIDtoTubeID[job.ID] = q.currentTube
	(q.tubes[q.currentTube]).Push(&job)
	return job.ID
}

//Watch put the tube in watch list
func (q *Queue) Watch(tbName string) (int, error) {
	var ok bool
	var tbID int
	tbID, ok = q.tubeNameToTubeID[tbName]
	if !ok {
		err := fmt.Sprintf("No tube with name %s", tbName)
		return len(q.watchList), errors.New(err)
	}
	_, ok = q.watchList[tbID]
	if !ok {
		q.watchList[tbID] = struct{}{}
	}
	return len(q.watchList), nil
}

func (q *Queue) push(tubeID int, job *types.Job) {
	q.jobIDtoTubeID[job.ID] = tubeID
	(q.tubes[tubeID]).Push(job)
}

//Reserve reserves the job from watch list
// and returns (chan jobID,chan data,chan error)
func (q *Queue) Reserve(jobID chan int, data chan []byte, err chan error) { //(chan int, chan []byte,chan error) {
	readyTubeID := -1
	highPriority := 1 << 32
	var priority int
	var ok error

	q.mux.Lock()
	for tbID := range q.watchList {
		priority, ok = (q.tubes[tbID]).Top()
		if (ok == nil) && (highPriority > priority) {
			highPriority = priority
			readyTubeID = tbID
		}
	}
	q.mux.Unlock()

	if readyTubeID == -1 {
		jobID <- 0
		data <- []byte{}
		err <- errors.New("No job in watchlist")
		return
		//return 0, []byte{}, errors.New("No job in watchlist")
	}

	q.mux.Lock()
	job, _ := (q.tubes[readyTubeID]).Pop()
	q.reserveTube[job.ID] = foo{readyTubeID, job}
	delete(q.jobIDtoTubeID, job.ID)
	q.mux.Unlock()

	jobID <- job.ID
	data <- job.Data
	err <- nil

	//sleep for TTR seconds
	time.Sleep(time.Duration(job.TTR) * time.Second)

	q.mux.Lock()
	defer q.mux.Unlock()
	f, ok2 := q.reserveTube[job.ID]
	if ok2 { //if job is running
		delete(q.reserveTube, job.ID)
		q.push(f.tubeID, job)
	}
	return
}

func (q *Queue) restoreJob(job *types.Job) {
	//sleep for TTR seconds
	time.Sleep(time.Duration(job.TTR) * time.Second)

	q.mux.Lock()
	f, ok := q.reserveTube[job.ID]
	if ok { //if job is running
		delete(q.reserveTube, job.ID)
		q.push(f.tubeID, job)
	}
	q.mux.Unlock()
}

//Delete deletes the job from queue and returns id of deleted job
//Does not support TTR
func (q *Queue) Delete(jobID int) (int, error) {
	q.mux.Lock()
	defer q.mux.Unlock()

	tubeID, ok := q.jobIDtoTubeID[jobID]
	if !ok { //if job is not in readytubes then it might be in reservetube
		_, ok = q.reserveTube[jobID]
		if !ok { //if job is not in reservetube
			err := fmt.Sprintf("No job with ID %d", jobID)
			return -1, errors.New(err)
		}
		delete(q.reserveTube, jobID)
	} else {
		_, _ = (q.tubes[tubeID]).Pop()
	}
	return jobID, nil
}
