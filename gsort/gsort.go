package gsort

import (
	"bufio"
	"compress/gzip"
	"container/heap"
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

type LineDeco struct {
	i    int // used internally to indicate which file it came from.
	line []byte
	Cols []int
}

type Lines []LineDeco

func (l Lines) Len() int {
	return len(l)
}

func (l Lines) Less(i, j int) bool {

	for k := 0; k < len(l[i].Cols); k++ {
		if l[j].Cols[k] == l[i].Cols[k] {
			continue
		}
		return l[i].Cols[k] < l[j].Cols[k]
	}
	return false
}
func (l Lines) Swap(i, j int) {
	if i < len(l) {
		l[j], l[i] = l[i], l[j]
	}
}

// for Heap

func (l *Lines) Push(i interface{}) {
	*l = append(*l, i.(LineDeco))
}

func (l *Lines) Pop() interface{} {
	n := len(*l)
	if n == 0 {
		return nil
	}
	v := (*l)[n-1]
	*l = (*l)[:n-1]
	return v
}

type Processor func(line []byte) LineDeco

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
		var chunk [][]byte
		chunk, rerr = readLines(brdr, memMB)
		if len(chunk) != 0 {
			f, err := ioutil.TempFile("", fmt.Sprintf("gsort.%d.", len(fileNames)))
			if err != nil {
				log.Fatal(err)
			}
			fileNames = append(fileNames, f.Name())
			defer os.Remove(f.Name())
			ch <- true
			wg.Add(1)
			// decorating and sorting is done in parallel.
			go sortAndWrite(f, wg, chunk, preprocess, ch)
		}
	}
	wg.Wait()
	if len(fileNames) == 1 {
		return writeOne(fileNames[0], bwtr)
	}
	// currently merging is serial. Should parallelize.
	return merge(fileNames, bwtr, preprocess)
}

func readLines(rdr *bufio.Reader, memMb int) ([][]byte, error) {

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
	processed := make([][]byte, n)
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
				return processed[:j], io.EOF
			}
		}
		processed[j] = line

		j += 1
		if err == io.EOF {
			return processed[:j], io.EOF
		}
		if j == len(processed) {
			break
		}
	}
	log.Printf("time to read: %d records: %.3f", len(processed), time.Since(start).Seconds())
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

	cache := make(Lines, len(fileNames))

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
			cache[i] = process(line)
			cache[i].line = line
			cache[i].i = i
		} else if err == io.EOF {
			continue
		} else if err != nil {
			return err
		}
	}

	heap.Init(&cache)

	for {
		o := heap.Pop(&cache)

		if o == nil {
			break
		}
		c := o.(LineDeco)
		// refill from same file
		line, err := fhs[c.i].ReadBytes('\n')
		if err != io.EOF && err != nil {
			return err
		}
		if len(line) != 0 {
			next := process(line)
			next.line = line
			next.i = c.i
			heap.Push(&cache, next)
		} else {
			log.Println(fileNames[c.i], string(line))
		}
		wtr.Write(c.line)

	}
	return nil
}

func sortAndWrite(f *os.File, wg *sync.WaitGroup, chunk [][]byte, process Processor, ch chan bool) {
	if len(chunk) == 0 {
		return
	}
	last := chunk[len(chunk)-1]
	if len(last) == 0 || last[len(last)-1] != '\n' {
		chunk[len(chunk)-1] = append(last, '\n')
	}
	defer wg.Done()
	gz := gzip.NewWriter(f)
	defer f.Close()
	defer gz.Close()
	dchunk := make(Lines, len(chunk))
	start := time.Now()

	for i, l := range chunk {
		dchunk[i] = process(l)
		dchunk[i].line = l
	}

	procTime := time.Since(start).Seconds()
	start = time.Now()

	sort.Sort(dchunk)
	sortTime := time.Since(start).Seconds()
	log.Printf("time to process: %.3f, time to sort: %.3f", procTime, sortTime)
	wtr := bufio.NewWriter(gz)
	for _, dl := range dchunk {
		wtr.Write(dl.line)
	}
	wtr.Flush()
	<-ch
	log.Println("wrote:", f.Name())
}
