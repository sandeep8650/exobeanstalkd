/*
 **********************
 * Sandeep Singh
 **********************
 */

package main

import (
	"bufio"
	exq "exotel.in/exobeanstalkd/exoqueue"
	"fmt"
	"os"
	"strconv"
	"strings"
)

//func New() Queue {
//func (q *Queue) Use(tbName string) string {
//func (q *Queue) Put(priority int, ttr int, count int, data []byte) (int, error) {
//func (q *Queue) Watch(tbName string) (int, error) {
//func (q *Queue) Reserve() (int, string, error) {
//func (q *Queue) Delete(jobID int) (int, error) {

func main() {
	q := exq.New()
	reader := bufio.NewReader(os.Stdin)
	for {
		reader.Reset(os.Stdin)
		cmd, _ := reader.ReadString('\n')
		cmdList := strings.Fields(cmd)
		n := len(cmdList)
		if n <= 0 {
			continue
		}
		cmdList[0] = strings.ToLower(cmdList[0])
		switch cmdList[0] {
		case "reserve":
			if n == 1 {
				jobID, data, err := q.Reserve()

				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Printf("RESERVED %d %s\n", jobID, data)
				}
			} else {
				fmt.Println("BAD FORMAT")
			}
		case "use":
			if n == 2 {
				tbName := q.Use(cmdList[1])
				fmt.Printf("USING %s\n", tbName)
			} else {
				fmt.Println("BAD FORMAT")
			}
		case "put":
			if n == 4 {
				priority, err := strconv.Atoi(cmdList[1])
				if err != nil {
					fmt.Println(err)
					break
				}
				ttr, err := strconv.Atoi(cmdList[2])
				if err != nil {
					fmt.Println(err)
					break
				}
				count, err := strconv.Atoi(cmdList[3])
				if err != nil {
					fmt.Println(err)
					break
				}
				data := make([]byte, 0)
				for i := 0; i < count; i++ {
					b, _ := reader.ReadByte()
					data = append(data, b)
				}
				jobID, err := q.Put(priority, ttr, count, data)
				if err != nil {
					fmt.Println(err)
					fmt.Println("BAD FORMAT")
				} else {
					fmt.Printf("INSERTED %d\n", jobID)
				}
			} else {
				fmt.Println("BAD FORMAT")
			}
		case "watch":
			if n == 2 {
				count, err := q.Watch(cmdList[1])
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Printf("WATCHING %d\n", count)
				}
			} else {
				fmt.Println("BAD FORMAT")
			}
		case "delete":
			if n == 2 {
				jobID, err := strconv.Atoi(cmdList[1])
				if err != nil {
					fmt.Println(err)
				} else {
					jobID, err = q.Delete(jobID)
					if err != nil {
						fmt.Println(err)
					} else {
						fmt.Printf("DELETED %d\n", jobID)
					}
				}
			} else {
				fmt.Println("BAD FORMAT")
			}
		default:
			fmt.Println("BAD FORMAT")
		}
	}
}
