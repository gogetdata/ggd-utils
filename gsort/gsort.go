package gsort

import (
	"bufio"
	"compress/flate"
	"fmt"
	"io/ioutil"

	gzip "github.com/klauspost/compress/gzip"
	//gzip "github.com/klauspost/pgzip"

	"container/heap"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
)

type LineDeco struct {
	i    int // used internally to indicate which file it came from.
	line []byte
	Cols []int
}

type Lines []LineDeco

func (l *Lines) Len() int {
	return len(*l)
}

func (l *Lines) Less(i, j int) bool {

	for k := 0; k < len((*l)[i].Cols); k++ {
		if (*l)[j].Cols[k] == (*l)[i].Cols[k] {
			continue
		}
		return (*l)[i].Cols[k] < (*l)[j].Cols[k]
	}
	return false
}
func (l *Lines) Swap(i, j int) {
	if i < len(*l) {
		(*l)[j], (*l)[i] = (*l)[i], (*l)[j]
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

type Processor func(line []byte) []int

func Sort(rdr io.Reader, wtr io.Writer, preprocess Processor, memMB int) error {

	brdr, bwtr := bufio.NewReader(rdr), bufio.NewWriter(wtr)
	defer bwtr.Flush()

	err := writeHeader(bwtr, brdr)
	if err != nil {
		return err
	}
	ch := make(chan Lines, runtime.GOMAXPROCS(-1))
	go readLines(ch, brdr, memMB)
	fileNames := writeChunks(ch, preprocess)

	log.Println(fileNames)
	for _, f := range fileNames {
		defer os.Remove(f)
	}

	if len(fileNames) == 1 {
		return writeOne(fileNames[0], bwtr)
	}
	return merge(fileNames, bwtr, preprocess)
}

func readLines(ch chan Lines, rdr *bufio.Reader, memMb int) {

	mem := 1000000 * memMb / runtime.GOMAXPROCS(-1)

	processed := make(Lines, 0, 500)
	var line []byte
	var err error

	sum := 0

	for {

		line, err = rdr.ReadBytes('\n')
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}

		if len(line) > 0 {
			processed = append(processed, LineDeco{line: line})
			sum += len(line)
		}

		if len(line) == 0 || err == io.EOF {
			np := len(processed)
			last := processed[np-1].line
			if len(last) == 0 || last[len(last)-1] != '\n' {
				processed[np-1].line = append(last, '\n')
			}
			ch <- processed
			break
		}

		if sum >= mem {
			ch <- processed
			processed = make(Lines, 0, 500)
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
			cache[i] = LineDeco{line: line, Cols: process(line), i: i}
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
			next := LineDeco{line: line, Cols: process(line), i: c.i}
			heap.Push(&cache, next)
		} else {
			os.Remove(fileNames[c.i])
		}
		wtr.Write(c.line)

	}
	return nil
}

func writeChunks(ch chan Lines, process Processor) []string {
	fileNames := make([]string, 0, 20)
	for chunk := range ch {
		f, err := ioutil.TempFile("", fmt.Sprintf("gsort.%d.", len(fileNames)))
		if err != nil {
			log.Fatal(err)
		}
		for _, c := range chunk {
			c.Cols = process(c.line)
		}

		L := Lines(chunk)
		sort.Sort(&L)

		gz, _ := gzip.NewWriterLevel(f, flate.BestSpeed)
		wtr := bufio.NewWriter(gz)
		for _, dl := range chunk {
			wtr.Write(dl.line)
			dl.Cols = nil
		}
		wtr.Flush()
		gz.Close()
		f.Close()
		fileNames = append(fileNames, f.Name())
	}
	return fileNames
}
