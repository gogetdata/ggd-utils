package gsort

import (
	"bufio"
	"container/heap"

	gzip "github.com/klauspost/pgzip"

	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

type Lines struct {
	lines [][]byte
	vals  [][]int
	idxs  []int // used in merge
}

func (l *Lines) Len() int {
	return len((*l).lines)
}

func (l *Lines) Less(i, j int) bool {

	for k := 0; k < len((*l).vals[i]); k++ {
		if (*l).vals[j][k] == (*l).vals[i][k] {
			continue
		}
		return (*l).vals[i][k] < (*l).vals[j][k]
	}
	return false
}
func (l *Lines) Swap(i, j int) {
	if i < len((*l).lines) {
		(*l).lines[j], (*l).lines[i] = (*l).lines[i], (*l).lines[j]
		(*l).vals[j], (*l).vals[i] = (*l).vals[i], (*l).vals[j]
	}
}

// for Heap
type lvi struct {
	line []byte
	vals []int
	idx  int
}

func (l *Lines) Push(i interface{}) {
	v := i.(*lvi)
	(*l).lines = append((*l).lines, v.line)
	(*l).vals = append((*l).vals, v.vals)
	(*l).idxs = append((*l).idxs, v.idx)
}

// Pop returns the index.
func (l *Lines) Pop() interface{} {
	n := len((*l).lines)
	if n == 0 {
		return nil
	}
	idx := (*l).idxs[n-1]
	(*l).lines = (*l).lines[:n-1]
	(*l).vals = (*l).vals[:n-1]
	(*l).idxs = (*l).idxs[:n-1]
	return idx
}

type Processor func(line []byte) []int

func Sort(rdr io.Reader, wtr io.Writer, preprocess Processor, memMB int) error {

	brdr, bwtr := bufio.NewReader(rdr), bufio.NewWriter(wtr)
	defer bwtr.Flush()

	err := writeHeader(bwtr, brdr)
	if err != nil {
		return err
	}
	// ch make sure we don't have too many processes running
	ch := make(chan bool, runtime.GOMAXPROCS(-1))
	// wg makes sure we wait until all is done
	wg := &sync.WaitGroup{}
	var rerr error
	fileNames := make([]string, 0)
	for rerr == nil {
		var chunk *Lines
		chunk, rerr = readLines(brdr, memMB, preprocess)
		if len(chunk.lines) != 0 {
			f, err := ioutil.TempFile("", fmt.Sprintf("gsort.%d.", len(fileNames)))
			if err != nil {
				log.Fatal(err)
			}
			fileNames = append(fileNames, f.Name())
			defer os.Remove(f.Name())
			ch <- true
			wg.Add(1)
			// decorating and sorting is done in parallel.
			go sortAndWrite(f, wg, chunk, ch, preprocess)
		}
	}
	wg.Wait()
	if len(fileNames) == 1 {
		return writeOne(fileNames[0], bwtr)
	}
	// currently merging is serial. Should parallelize.
	return merge(fileNames, bwtr, preprocess)
}

func readLines(rdr *bufio.Reader, memMb int, process Processor) (*Lines, error) {

	start := time.Now()

	N := 2000
	lens, j := make([]int, 0, N), 0

	// sample XX lines to get the average length.
	lines := make([][]byte, 0, N)
	for len(lines) < cap(lines) {
		line, err := rdr.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
		if len(line) != 0 {
			lens = append(lens, len(line))
			lines = append(lines, line)
		}
		if err == io.EOF {
			break
		}
	}
	sort.Ints(lens)

	n := 1000000 * memMb / lens[int(0.95*float64(len(lens)))] / runtime.GOMAXPROCS(-1)
	if n < 1 {
		n = 1
	}

	processed := &Lines{lines: make([][]byte, n), vals: make([][]int, n)}
	var line []byte
	var err error

	for {

		if j < len(lines) {
			line = lines[j]
		} else {
			line, err = rdr.ReadBytes('\n')
			if err != nil && err != io.EOF {
				return nil, err
			}
		}
		if len(line) == 0 {
			if err == io.EOF {
				last := processed.lines[len(processed.lines)-1]
				if len(last) == 0 || last[len(last)-1] != '\n' {
					processed.lines[len(processed.lines)-1] = append(line, '\n')
				}
				processed.lines = processed.lines[:j]
				processed.vals = processed.vals[:j]
				return processed, io.EOF
			}
		}
		//processed.vals[j] = process(line)
		processed.lines[j] = line

		j += 1
		if err == io.EOF {
			processed.lines = processed.lines[:j]
			processed.vals = processed.vals[:j]
			return processed, io.EOF
		}
		if j == len(processed.lines) {
			break
		}
	}
	log.Printf("time to read: %d records: %.3f", len(processed.lines), time.Since(start).Seconds())
	return processed, nil
}

func writeHeader(wtr *bufio.Writer, rdr *bufio.Reader) error {
	for {
		b, err := rdr.Peek(1)
		if err != nil {
			return err
		}
		if b[0] != '#' {
			break
		}
		line, err := rdr.ReadBytes('\n')
		if err != nil {
			return err
		}
		wtr.Write(line)
	}
	return nil
}

// fast path where we don't use merge if it all fit in memory.
func writeOne(fname string, wtr io.Writer) error {
	rdr, err := os.Open(fname)
	if err != nil {
		return err
	}
	defer rdr.Close()
	gz, err := gzip.NewReader(rdr)
	if err != nil {
		return err
	}
	_, err = io.Copy(wtr, gz)
	return err
}

func merge(fileNames []string, wtr io.Writer, process Processor) error {
	fhs := make([]*bufio.Reader, len(fileNames))
	cache := &Lines{lines: make([][]byte, len(fileNames)),
		vals: make([][]int, len(fileNames)),
		idxs: make([]int, len(fileNames))}

	for i, fn := range fileNames {
		fh, err := os.Open(fn)
		if err != nil {
			return err
		}
		defer fh.Close()
		gz, err := gzip.NewReader(fh)
		if err != nil {
			return err
		}
		defer gz.Close()
		fhs[i] = bufio.NewReader(gz)

		line, err := fhs[i].ReadBytes('\n')
		if len(line) > 0 {
			cache.vals[i] = process(line)
			cache.lines[i] = line
			cache.idxs[i] = i
		} else if err == io.EOF {
			continue
		} else if err != nil {
			return err
		}
	}

	heap.Init(cache)

	for {
		o := heap.Pop(cache)

		if o == nil {
			break
		}
		idx := o.(int)
		// refill from same file
		line, err := fhs[idx].ReadBytes('\n')
		if err != io.EOF && err != nil {
			return err
		}
		if len(line) != 0 {
			next := &lvi{line, process(line), idx}
			heap.Push(cache, next)
		} else {
			os.Remove(fileNames[idx])
		}
		wtr.Write(line)

	}
	return nil
}

func sortAndWrite(f *os.File, wg *sync.WaitGroup, chunk *Lines, ch chan bool, process Processor) {
	if len(chunk.lines) == 0 {
		return
	}
	defer wg.Done()
	gz := gzip.NewWriter(f)
	defer f.Close()
	defer gz.Close()

	start := time.Now()
	for i, line := range chunk.lines {
		chunk.vals[i] = process(line)
	}

	sort.Sort(chunk)
	chunk.vals = nil
	sortTime := time.Since(start).Seconds()
	log.Printf("time to sort: %.3f", sortTime)
	wtr := bufio.NewWriter(gz)
	for _, buf := range chunk.lines {
		wtr.Write(buf)
		buf = nil
	}
	chunk.lines = nil
	wtr.Flush()
	<-ch
	log.Println("wrote:", f.Name())
}
