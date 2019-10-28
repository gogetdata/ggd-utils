// read genome files
package ggd_utils

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/brentp/xopen"
)

// GenomeFile has the Lengths and Orders of the chromosomes.
type GenomeFile struct {
	Lengths map[string]int
	Order   map[string]int
	path    string
	ReMap   map[string]string
}

// Less checks if one chromosome occurs before the other.
func (g *GenomeFile) Less(a, b string) bool {
	return g.Order[a] <= g.Order[b]
}

func readChromosomMappings(fname string) map[string]string {
	if fname == "" {
		return nil
	}
	rdr, err := xopen.Ropen(fname)
	result := make(map[string]string)
	if err != nil {
		log.Fatalf("[gsort] unable to open chromosome maping file: %s", fname)
	}

	warned := false
	for {
		line, err := rdr.ReadString('\n')
		if len(line) > 0 {
			toks := strings.Split(strings.TrimSpace(line), "\t")
			if len(toks) == 1 {
				toks = append(toks, "[unknown]"+toks[0])
				if !warned {
					log.Printf("[gsort] warning unmappable chromosome: %s.", toks[0])
					warned = true
				}
			}
			result[toks[0]] = toks[1]
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("[gsort] error reading chromosome maping file: %s. %s", fname, err)
		}
	}
	return result
}

// ReadGenomeFile returns a GenomeFile struct
func ReadGenomeFile(path string, chromsomeMappings string) (*GenomeFile, error) {

	rdr, err := xopen.Ropen(path)
	if err != nil {
		return nil, err
	}
	gf := &GenomeFile{path: path, Lengths: make(map[string]int, 50), Order: make(map[string]int, 50)}
	defer rdr.Close()
	gf.ReMap = readChromosomMappings(chromsomeMappings)

	space := regexp.MustCompile("\\s+")
	// allow us to bypass a header. found indicates when we have a usable line.
	found := false

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
			if !found {
				continue
			}
			return nil, err
		}
		found = true
		gf.Lengths[chrom] = length
		gf.Order[chrom] = len(gf.Order)

	}
	if !found {
		return nil, fmt.Errorf("no usable lengths found for %s\n", path)
	}
	return gf, nil
}
