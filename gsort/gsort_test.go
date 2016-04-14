package gsort_test

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	"github.com/brentp/ggd-utils/gsort"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type GSortTest struct{}

var _ = Suite(&GSortTest{})

func (s *GSortTest) TestSort(c *C) {

	data := strings.NewReader(`a\t1
b\t2
a\t3`)

	pp := func(line []byte) gsort.LineDeco {
		l := gsort.LineDeco{Cols: make([]int, 2)}
		toks := bytes.Split(line, []byte{'\t'})
		l.Cols[0] = int(toks[0][0])
		if len(toks) > 1 {
			v, err := strconv.Atoi(string(toks[1]))
			if err != nil {
				l.Cols[1] = -1
			} else {
				l.Cols[1] = v
			}
		} else {
			l.Cols[1] = -1
		}
		return l

	}
	b := make([]byte, 0, 20)
	wtr := bytes.NewBuffer(b)

	err := gsort.Sort(data, wtr, pp, 22)
	c.Assert(err, IsNil)

	c.Assert(wtr.String(), Equals, `a\t1
a\t3
b\t2
`)

}
