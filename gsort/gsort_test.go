package gsort_test

import (
	"strings"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type GSortTest struct{}

var _ = Suite(&GSortTest{})

func (s *GSortTest) TestSort(c *C) {

	data := strings.NewReader(`a\t1\nb\t2\na\t3`)

	pp := func(line []byte) LineDeco {
        l := LineDeco{Cols:make([]int, 2}
		toks := bytes.Split(line, []byte('\t'))
		if toks[0][0] == 'a' {
			l[0] = 0
		}
		
	}

	Sortkk

}
