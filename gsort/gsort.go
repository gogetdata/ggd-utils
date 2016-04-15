package gsort

import (
	"bufio"
	"compress/flate"
	"fmt"
	"io/ioutil"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"syscall"
	"time"

	gzip "github.com/klauspost/compress/gzip"
	//gzip "github.com/klauspost/pgzip"

	"container/heap"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
)

type Chunk struct {
	lines  [][]byte
	idxs   []int // only for heap
	lessFn func(a, b []byte) bool
}

func (c *Chunk) Len() int {
	return len(c.lines)
}

func (c *Chunk) Less(i, j int) bool {
	return c.lessFn(c.lines[i], c.lines[j])
}

func (c *Chunk) Swap(i, j int) {
	if i < len(c.lines) {
		(*c).lines[j], (*c).lines[i] = (*c).lines[i], (*c).lines[j]
	}
	if i < len(c.idxs) {
		(*c).idxs[j], (*c).idxs[i] = (*c).idxs[i], (*c).idxs[j]
	}
}

// for Heap

type Pair struct {
	line []byte
	idx  int
}

func (c *Chunk) Push(i interface{}) {
	p := i.(Pair)
	(*c).lines = append((*c).lines, p.line)
	(*c).idxs = append((*c).idxs, p.idx)
}

func (c *Chunk) Pop() interface{} {
	n := len(c.lines)
	if n == 0 {
		return nil
	}
	line := (*c).lines[n-1]
	(*c).lines = (*c).lines[:n-1]
	idx := (*c).idxs[n-1]
	(*c).idxs = (*c).idxs[:n-1]
	return Pair{line, idx}
}

func Sort(rdr io.Reader, wtr io.Writer, lessFn func(a, b []byte) bool, memMB int) error {

	f, perr := os.Create("gsort.pprof")
	if perr != nil {
		panic(perr)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	brdr, bwtr := bufio.NewReader(rdr), bufio.NewWriter(wtr)
	defer bwtr.Flush()

	err := writeHeader(bwtr, brdr)
	if err != nil {
		return err
	}
	ch := make(chan [][]byte, runtime.GOMAXPROCS(-1))
	go readLines(ch, brdr, memMB)
	fileNames := writeChunks(ch, lessFn)

	for _, f := range fileNames {
		defer os.Remove(f)
	}

	if len(fileNames) == 1 {
		return writeOne(fileNames[0], bwtr)
	}
	// TODO have special merge for when stuff is already mostly sorted. don't need pri queue.
	return merge(fileNames, bwtr, lessFn)
}

func readLines(ch chan [][]byte, rdr *bufio.Reader, memMb int) {

	mem := 1000000 * memMb / runtime.GOMAXPROCS(-1)

	lines := make([][]byte, 0, 500)
	var line []byte
	var err error

	sum := 0

	for {

		line, err = rdr.ReadBytes('\n')
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}

		if len(line) > 0 {
			lines = append(lines, line)
			sum += len(line)
		}

		if len(line) == 0 || err == io.EOF {
			np := len(lines)
			last := lines[np-1]
			if len(last) == 0 || last[len(last)-1] != '\n' {
				lines[np-1] = append(last, '\n')
			}
			ch <- lines
			break
		}

		if sum >= mem {
			ch <- lines
			lines = make([][]byte, 0, 500)
			sum = 0
		}
	}
	close(ch)
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

func merge(fileNames []string, wtr io.Writer, lessFn func(a, b []byte) bool) error {

	start := time.Now()

	fhs := make([]*bufio.Reader, len(fileNames))

	cache := Chunk{lines: make([][]byte, len(fileNames)),
		lessFn: lessFn,
		idxs:   make([]int, len(fileNames))}

	for i, fn := range fileNames {
		fh, err := os.Open(fn)
		if err != nil {
			return err
		}
		defer fh.Close()
		//gz, err := newFastGzReader(fh)
		gz, err := gzip.NewReader(fh)
		if err != nil {
			return err
		}
		defer gz.Close()
		fhs[i] = bufio.NewReader(gz)

		line, err := fhs[i].ReadBytes('\n')
		if len(line) > 0 {
			cache.lines[i] = line
			cache.idxs[i] = i
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
		c := o.(Pair)
		// refill from same file
		line, err := fhs[c.idx].ReadBytes('\n')
		if err != io.EOF && err != nil {
			return err
		}
		if len(line) != 0 {
			next := Pair{line: line, idx: c.idx}
			heap.Push(&cache, next)
		} else {
			os.Remove(fileNames[c.idx])
		}
		wtr.Write(c.line)

	}

	log.Printf("time to merge: %.3f", time.Since(start).Seconds())
	return nil
}

func init() {
	// make sure we don't leave any temporary files.
	c := make(chan os.Signal, 1)
	pid := os.Getpid()
	signal.Notify(c,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-c
		matches, err := filepath.Glob(filepath.Join(os.TempDir(), fmt.Sprintf("gsort.%d.*", pid)))
		if err != nil {
			log.Fatal(err)
		}
		for _, m := range matches {
			os.Remove(m)
		}
		os.Exit(3)
	}()

}

func writeChunks(ch chan [][]byte, cmp func(a, b []byte) bool) []string {
	fileNames := make([]string, 0, 20)
	pid := os.Getpid()
	for lines := range ch {
		f, err := ioutil.TempFile("", fmt.Sprintf("gsort.%d.%d.", pid, len(fileNames)))
		if err != nil {
			log.Fatal(err)
		}
		chunk := Chunk{lines: lines, lessFn: cmp}
		sort.Sort(&chunk)

		gz, _ := gzip.NewWriterLevel(f, flate.BestSpeed)
		wtr := bufio.NewWriterSize(gz, 65536)
		for _, line := range chunk.lines {
			wtr.Write(line)
		}
		wtr.Flush()
		gz.Close()
		f.Close()
		fileNames = append(fileNames, f.Name())
	}
	return fileNames
}
