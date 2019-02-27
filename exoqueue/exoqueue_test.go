/*
 **********************
 * Sandeep Singh
 **********************
 */

package exoqueue

import (
	//"errors"
	"testing"
)

type testCase struct {
}

func TestFIFOSingleTube(t *testing.T) {
	q := New()
	out := []int{}
	for i := 0; i < 1000; i++ {
		id, err := q.Put(0, 5, 7, []byte("testing"))
		if err != nil {
			t.Error(err)
		}
		out = append(out, id)
	}
	for i := 0; i < 1000; i++ {
		id, data, err := q.Reserve()
		q.Delete(id)
		if err != nil {
			t.Error(err)
		} else if id != out[i] || string(data) != "testing" {
			t.Errorf("Wants %d==%d and %s==testing", id, out[i], string(data))
		}
	}
}

/*func TestMultipleTube(t *testing.T) {
	q:=New()

	for t:=0;t<10;t++{
		tbName:="Tube"+strconv.Atoi(t)
		q.Use(tbName)
		for i:=0;i<1000;i++{
			q.Put()
		}
	}
}*/

func BenchmarkPut(b *testing.B) {
	q := New()
	data := make([]byte, 7)
	for i := 0; i < b.N; i++ {
		_, err := q.Put(0, 5, 7, data)
		if err != nil {
			b.Error(err)
		}
	}
}
