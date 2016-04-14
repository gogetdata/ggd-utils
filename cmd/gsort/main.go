package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

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

func sortFnFromCols(cols []int, gf *ggd_utils.GenomeFile) func([]byte) gsort.LineDeco {
	m := 0
	for _, c := range cols {
		if c > m {
			m = c
		}
	}
	m += 2
	fn := func(line []byte) gsort.LineDeco {
		l := gsort.LineDeco{Cols: make([]int, len(cols))}
		// handle chromosome column
		toks := bytes.SplitN(line, []byte{'\t'}, m)
		var ok bool
		// TODO: use unsafe string
		l.Cols[0], ok = gf.Order[toks[cols[0]]]
		if !ok {
			log.Fatalf("unknown chromosome: %s", toks[cols[0]])
		}

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
						v, err := strconv.Atoi(strings.TrimSuffix(toks[c], "\r\n"))
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
					return "", nil, fmt.Errorf("unknown file format")
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

	gf, err := ggd_utils.ReadGenomeFile(args.Genome)
	if err != nil {
		log.Fatal(err)
	}

}
