package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/alexflint/go-arg"
	"github.com/biogo/hts/bgzf"
	"github.com/brentp/xopen"
	ggd_utils "github.com/gogetdata/ggd-utils"
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

type chromStartGetter func([]byte) ([]byte, int, error)

var get_vcf_chrom_start chromStartGetter = func(line []byte) ([]byte, int, error) {
	cpr := bytes.SplitN(line, []byte{'\t'}, 3)
	chrom := cpr[0]
	pos, err := strconv.Atoi(string(cpr[1]))
	return chrom, pos, err
}

var get_gff_chrom_start chromStartGetter = func(line []byte) ([]byte, int, error) {
	toks := bytes.SplitN(line, []byte{'\t'}, 5)
	start, err := strconv.Atoi(string(toks[3]))
	return toks[0], start, err
}

func main() {

	p := arg.MustParse(&args)
	if args.Path == "" || args.Genome == "" {
		p.Fail("must specify a path to check and a genome file")
	}
	gf, err := ggd_utils.ReadGenomeFile(args.Genome, "")
	if err != nil {
		log.Fatal(err)
	}

	if strings.HasSuffix(args.Path, ".vcf.gz") {
		checkVCF(args.Path, gf)
	} else if strings.HasSuffix(args.Path, ".bed.gz") {
		checkTab(args.Path, gf, get_vcf_chrom_start)
	} else {
		found := false
		for _, suff := range []string{"gff", "gtf", "gff3", "gff2"} {
			if strings.HasSuffix(args.Path, suff) || strings.HasSuffix(args.Path, suff+".gz") {
				found = true
				checkTab(args.Path, gf, get_gff_chrom_start)
				break
			}
		}
		if !found {
			log.Fatalf("Don't know how to check this type of file: %s\n", args.Path)
		}
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
				return chrom, start, fmt.Errorf("chromosomes not in specified sort order: %s, %s at line %d\nuse gsort (https://github.com/brentp/gsort/) to order according to the genome file", lastChrom, chrom, iline)
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

func checkTab(path string, gf *ggd_utils.GenomeFile, getter chromStartGetter) {
	if !(xopen.Exists(path+".tbi") || xopen.Exists(path+".csi")) {
		log.Fatalf("BED: %s should have a .tbi", path)
	}
	fh, err := os.Open(path)
	if err != nil {
		log.Fatalf("BED: unable to open file: %s", path)
	}
	if ok, _ := bgzf.HasEOF(fh); !ok {
		log.Fatal("missing EOF")
	}

	bgz, err := bgzf.NewReader(fh, 1)
	defer bgz.Close()
	defer fh.Close()

	rdr := bufio.NewReader(bgz)
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
		if len(line) == 0 {
			continue
		}
		if line[0] == '#' || bytes.HasPrefix(line, []byte("track")) || bytes.HasPrefix(line, []byte("browser")) {
			if afterHeader {
				log.Fatalf("found comment/header line after header at line: %d", iline)
			}
		} else {
			afterHeader = true
			lastChrom, lastPos, err = checkLine(iline, line, lastChrom, lastPos, getter, gf.Less)
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
	if !(xopen.Exists(path+".tbi") || xopen.Exists(path+".csi")) {
		log.Fatal("VCF should have a .tbi or .csi")
	}

	fh, err := os.Open(path)
	check(err)
	defer fh.Close()
	if ok, _ := bgzf.HasEOF(fh); !ok {
		log.Fatalf("missing EOF in %s", path)
	}

	bgz, err := bgzf.NewReader(fh, 1)
	check(err)
	defer bgz.Close()
	rdr := bufio.NewReader(bgz)

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
