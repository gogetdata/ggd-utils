package main

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MTest struct{}

var _ = Suite(&MTest{})

func less(a, b string) bool {
	return a < b
}

var lineTests = []struct {
	iline     int
	line      []byte
	lastChrom []byte
	lastPos   int
	getter    chromStartGetter
	cmp       func(a, b string) bool
	expChrom  []byte
	expStart  int
	expError  bool
}{
	{1, []byte("1\t23\tasdf\tvvv"), []byte("1"), 22, get_vcf_chrom_start, less, []byte("1"), 23, false},
	{1, []byte("1\t23\tasdf\tvvv"), []byte("11"), 22, get_vcf_chrom_start, less, []byte("1"), 23, true},
	{1, []byte("1\t23\tasdf\tvvv"), []byte("1"), 24, get_vcf_chrom_start, less, []byte("1"), 23, true},

	{1, []byte("1\t23\tasdf\tvvv"), []byte("1"), 24, get_gff_chrom_start, less, []byte("1"), 0, true},

	{1, []byte("1111\tex\tmRNA\t1235\t1236"), []byte("1"), 24, get_gff_chrom_start, less, []byte("1111"), 1235, false},
}

func (s *MTest) TestCases(c *C) {
	for _, t := range lineTests {

		chrom, start, err := checkLine(t.iline, t.line, t.lastChrom, t.lastPos, t.getter, t.cmp)
		c.Assert(err != nil, Equals, t.expError)
		c.Assert(chrom, DeepEquals, t.expChrom)
		c.Assert(start, Equals, t.expStart)
	}
}
