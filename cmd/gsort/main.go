package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"unsafe"

	"github.com/alexflint/go-arg"
	"github.com/brentp/ggd-utils"
	"github.com/brentp/ggd-utils/gsort"
	"github.com/brentp/xopen"
)

var DEFAULT_MEM int = 2000

var FileCols map[string][]int = map[string][]int{
	"BED": []int{0, 1, 2},
	"VCF": []int{0, 1, -1},
	"GFF": []int{0, 3, 4},
	"GTF": []int{0, 3, 4},
}

var CHECK_ORDER = []string{"BED", "GTF"}

var args struct {
	Path   string `arg:"positional,help:a tab-delimited file to sort"`
	Genome string `arg:"positional,help:a genome file of chromosome sizes and order"`
	Memory int    `arg:"-m,help:megabytes of memory to use before writing to temp files."`
}

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func max(ints []int) int {
	m := 0
	for _, c := range ints {
		if c > m {
			m = c
		}
	}
	return m
}

type LesserFunc func(a, b []byte) bool

// the last function is used when a column is -1
func sortFnFromCols(cols []int, gf *ggd_utils.GenomeFile, getter *func(int, [][]byte) int) LesserFunc {
	// cols are ordered indexes of chrom, start, end
	m := 2 + max(cols)
	if getter != nil && m < 6 {
		m = 6
	}

	type cache struct {
		line []byte
		toks [][]byte
		vals []int
	}

	lastA := cache{vals: make([]int, len(cols))}
	lastB := cache{vals: make([]int, len(cols))}
	fn := func(a, b []byte) bool {
		// handle chromosome column
		var toksA, toksB [][]byte
		if bytes.Equal(lastA.line, a) {
			toksA = lastA.toks
		} else {
			toksA = bytes.SplitN(a, []byte{'\t'}, m)
			lastA.toks = toksA
			lastA.line = a
		}
		if bytes.Equal(lastB.line, b) {
			toksB = lastB.toks
		} else {
			toksB = bytes.SplitN(b, []byte{'\t'}, m)
			lastB.toks = toksB
			lastB.line = b
		}

		if !bytes.Equal(toksA[cols[0]], toksB[cols[0]]) {

			aChrom, ok := gf.Order[unsafeString(toksA[cols[0]])]
			if !ok {
				log.Fatalf("unknown chromosome: %s", toksA[cols[0]])
			}
			bChrom, ok := gf.Order[unsafeString(toksB[cols[0]])]
			if !ok {
				log.Fatalf("unknown chromosome: %s", toksB[cols[0]])
			}
			if aChrom != bChrom {
				return aChrom < bChrom
			}
		}

		aStart, err := strconv.Atoi(unsafeString(toksA[cols[1]]))
		if err != nil {
			log.Fatal(err)
		}
		bStart, err := strconv.Atoi(unsafeString(toksB[cols[1]]))
		if err != nil {
			log.Fatal(err)
		}
		if aStart != bStart {
			return aStart < bStart
		}

		eCol := cols[2]
		var aEnd, bEnd int
		if eCol == -1 {
			aEnd = (*getter)(aStart, toksA)
			bEnd = (*getter)(bStart, toksB)
		} else {
			var err error
			aEnd, err = strconv.Atoi(unsafeString(toksA[eCol]))
			if err != nil {
				log.Fatal(err)
			}
			bEnd, err = strconv.Atoi(unsafeString(toksB[eCol]))
			if err != nil {
				log.Fatal(err)
			}
		}
		return aEnd <= bEnd
	}
	return fn
}

func sniff(rdr *bufio.Reader) (string, *bufio.Reader, error) {
	lines := make([]string, 0, 200)
	var ftype string
	for len(lines) < 50000 {
		line, err := rdr.ReadString('\n')
		if len(line) > 0 {
			lines = append(lines, line)
			if line[0] == '#' {
				if strings.HasPrefix(line, "##fileformat=VCF") || strings.HasPrefix(line, "#CHROM\tPOS\tID") {
					ftype = "VCF"
					break
				} else {
					continue
				}
			} else {
				toks := strings.Split(line, "\t")
				if len(toks) < 3 {
					return "", nil, fmt.Errorf("file has fewer than 3 columns")
				}
				for _, t := range CHECK_ORDER {
					cols := FileCols[t]
					ok := true
					last := 0
					for _, c := range cols[1:] {
						if c >= len(toks) {
							ok = false
							break
						}
						v, err := strconv.Atoi(strings.TrimRight(toks[c], "\r\n"))
						if err != nil {
							ok = false
							break
						}
						// check that 0 <= start col <= end_col
						if v < last {
							ok = false
							break
						}
						last = v
					}
					if ok {
						ftype = t
						break
					}
				}
				if ftype == "" {
					return "", nil, fmt.Errorf("unknown file format: %s", string(line))
				} else {
					break
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				return "", nil, err
			}
		}

	}
	nrdr := io.MultiReader(strings.NewReader(strings.Join(lines, "")), rdr)
	return ftype, bufio.NewReader(nrdr), nil
}

func find(key []byte, info []byte) (int, int) {
	l := len(key)
	if pos := bytes.Index(info, key); pos != -1 {
		var end int
		for end = pos + l + 1; end < len(info); end++ {
			if info[end] == ';' {
				break
			}
		}
		return pos + l, end
	}
	return -1, -1

}

func getMax(i []byte) (int, error) {
	if !bytes.Contains(i, []byte(",")) {
		return strconv.Atoi(unsafeString(i))
	}
	all := bytes.Split(i, []byte{','})
	max := -1
	for _, b := range all {
		v, err := strconv.Atoi(unsafeString(b))
		if err != nil {
			return max, err
		}
		if v > max {
			max = v
		}
	}
	return max, nil
}

var vcfEndGetter func(int, [][]byte) int = func(start int, toks [][]byte) int {
	if bytes.Contains(toks[4], []byte{'<'}) && (bytes.Contains(toks[4], []byte("<DEL")) ||
		bytes.Contains(toks[4], []byte("<DUP")) ||
		bytes.Contains(toks[4], []byte("<INV")) ||
		bytes.Contains(toks[4], []byte("<CN"))) {
		// need to look at INFO for this.
		var info []byte
		if len(toks) < 8 {
			// just grab everything since we look for end= anyway
			info = toks[len(toks)-1]
		} else {
			info = toks[7]
		}
		if s, e := find([]byte("END="), info); s != -1 {
			end, err := getMax(info[s:e])
			if err != nil {
				log.Fatal(err)
			}
			return end
		}
		s, e := find([]byte("SVLEN="), info)
		if s == -1 {
			log.Println("warning: cant find end for %s", string(info))
			return start + len(toks[3])
		}
		svlen, err := getMax(info[s:e])
		if err != nil {
			log.Fatal(err)
		}
		return start + svlen

	} else {
		// length of reference.
		return start + len(toks[3])
	}

}

func main() {

	args.Memory = DEFAULT_MEM
	p := arg.MustParse(&args)
	if args.Path == "" || args.Genome == "" {
		p.Fail("must specify a tab-delimited file and a genome file")
	}

	rdr, err := xopen.Ropen(args.Path)
	if err != nil {
		log.Fatal(err)
	}
	defer rdr.Close()

	ftype, brdr, err := sniff(rdr.Reader)
	if err != nil {
		log.Fatal(err)
	}

	gf, err := ggd_utils.ReadGenomeFile(args.Genome)
	if err != nil {
		log.Fatal(err)
	}

	getter := &vcfEndGetter
	if ftype != "VCF" {
		getter = nil
	}

	sortFn := sortFnFromCols(FileCols[ftype], gf, getter)
	wtr := bufio.NewWriter(os.Stdout)

	if err := gsort.Sort(brdr, wtr, sortFn, args.Memory); err != nil {
		log.Fatal(err)
	}
}
