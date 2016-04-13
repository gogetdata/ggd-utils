// read genome files
package ggd_utils

import (
	"bytes"
	"io"
	"regexp"
	"strconv"

	"github.com/brentp/xopen"
)

type GenomeFile struct {
	Lengths map[string]int
	order   map[string]int
	path    string
}

// Less checks if one chromosome occurs before the other.
func (g *GenomeFile) Less(a, b string) bool {
	return g.order[a] <= g.order[b]
}

func ReadGenomeFile(path string) (*GenomeFile, error) {

	rdr, err := xopen.Ropen(path)
	if err != nil {
		return nil, err
	}
	gf := &GenomeFile{path: path, Lengths: make(map[string]int, 50), order: make(map[string]int, 50)}
	defer rdr.Close()

	space := regexp.MustCompile("\\s+")

	for {
		line, err := rdr.ReadBytes('\n')
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			if line[0] == '#' {
				continue
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(line) == 0 {
			continue
		}

		toks := space.Split(string(line), -1)
		chrom := toks[0]
		length, err := strconv.Atoi(toks[1])
		if err != nil {
			return nil, err
		}
		gf.Lengths[chrom] = length
		gf.order[chrom] = len(gf.order)

	}
	return gf, nil
}
