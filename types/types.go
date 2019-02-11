/*
 **********************
 * Sandeep Singh
 **********************
 */

package types

//Job structure
//ID: job id
//Priority: priority of job
//TTR: time to run
//Data: job byte data
//Index: require for heap operations
type Job struct {
	ID       int
	Priority int
	TTR      int
	Data     []byte
	Index    int
}
