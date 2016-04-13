package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/alexflint/go-arg"
	"github.com/brentp/ggd-utils"
	"github.com/brentp/xopen"
)

var args struct {
	Path   string `arg:"positional"`
	Genome string `arg:"-g,required,help:a genome file of chromosome sizes and order"`
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

var VCFCommentAfterHeader = errors.New("comment line after non-header #CHROM line in VCF")

type chromStartFunc func(line []byte) (chrom []byte, end int, err error)

var get_vcf_chrom_start chromStartFunc = func(line []byte) ([]byte, int, error) {
	cpr := bytes.SplitN(line, []byte{'\t'}, 3)
	chrom := cpr[0]
	pos, err := strconv.Atoi(string(cpr[1]))
	return chrom, pos, err
}

func main() {

	p := arg.MustParse(&args)
	if args.Path == "" || args.Genome == "" {
		p.Fail("must specify a path to check and a genome file")
	}
	gf, err := ggd_utils.ReadGenomeFile(args.Genome)
	if err != nil {
		log.Fatal(err)
	}

	if strings.HasSuffix(args.Path, ".vcf.gz") {
		checkVCF(args.Path, gf)
	} else if strings.HasSuffix(args.Path, ".bed.gz") {
		checkBED(args.Path, gf)

	} else {
		log.Fatalf("Don't know how to check this type of file: %s\n", args.Path)
	}
}

func checkLine(iline int, line []byte, lastChrom []byte, lastStart int,
	get_chrom_start func([]byte) ([]byte, int, error),
	cmp func(a, b string) bool) ([]byte, int, error) {

	chrom, start, err := get_chrom_start(line)
	if err != nil {
		return chrom, start, err
	}
	if !bytes.Equal(chrom, lastChrom) {
		if len(lastChrom) != 0 {
			if !cmp(string(lastChrom), string(chrom)) {
				return chrom, start, fmt.Errorf("chromosomes not in specified sort order: %s, %s at line %d\n", lastChrom, chrom, iline)
			}
		}
		lastChrom = chrom
		lastStart = start
	} else {
		if start < lastStart {
			return chrom, start, fmt.Errorf("positions not sorted: %d => %d at line %d\n", lastStart, start, iline)
		}
		lastStart = start
	}
	if start < 0 {
		return chrom, start, fmt.Errorf("negative position at line: %d (%d)", iline, start)
	}

	return chrom, start, nil
}

func checkBED(path string, gf *ggd_utils.GenomeFile) {
	if !xopen.Exists(path + ".tbi") {
		log.Fatalf("BED: %s should have a .tbi", path)
	}
	rdr, err := xopen.Ropen(path)
	iline := 1
	check(err)
	afterHeader := false
	lastChrom := []byte("")
	lastPos := -1
	for {
		line, err := rdr.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		// TODO handle track lines?
		if line[0] == '#' {
			if afterHeader {
				log.Fatalf("found comment line after header at line: %d", iline)
			}
		} else {
			afterHeader = true
			lastChrom, lastPos, err = checkLine(iline, line, lastChrom, lastPos, get_vcf_chrom_start, gf.Less)
			if err != nil {
				log.Fatal(err)
			}
			if gf.Lengths[string(lastChrom)] < lastPos {
				log.Fatalf("position: %d beyond end of chromosome %s", lastPos, lastChrom)
			}
		}
		iline += 1
	}
}

func checkVCF(path string, gf *ggd_utils.GenomeFile) {
	if !xopen.Exists(path + ".tbi") {
		log.Fatal("VCF should have a .tbi")
	}

	rdr, err := xopen.Ropen(path)
	check(err)
	afterHeader := false
	iline := 1
	lastChrom := []byte("")
	lastPos := -1

	for {
		line, err := rdr.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if iline == 1 {
			if !bytes.HasPrefix(line, []byte("##fileformat=VCF")) {
				log.Println(string(line))
				log.Fatal("VCF header line '##fileformat=VCF... not found")
			}
		}
		if line[0] == '#' {
			if bytes.HasPrefix(line, []byte("#CHROM\t")) {
				afterHeader = true
			} else if afterHeader {
				log.Fatal(VCFCommentAfterHeader, iline)
			}
		} else {
			if !afterHeader {
				log.Fatal("VCF header line '#CHROM\t ... not found")
			}
			lastChrom, lastPos, err = checkLine(iline, line, lastChrom, lastPos, get_vcf_chrom_start, gf.Less)
			if err != nil {
				log.Fatal(err)
			}
			if gf.Lengths[string(lastChrom)] < lastPos {
				log.Fatalf("position: %d beyond end of chromosome %s", lastPos, lastChrom)
			}
		}
		iline += 1
	}
}
