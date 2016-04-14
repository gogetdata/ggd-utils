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

	data := strings.NewReader(`a\t1\nb\t2\na\t3`)

	pp := func(line []byte) gsort.LineDeco {
		l := gsort.LineDeco{Cols: make([]int, 2)}
		toks := bytes.Split(line, []byte{'\t'})
		l.Cols[0] = int(toks[0][0])
		v, err := strconv.Atoi(string(toks[1]))
		if err != nil {
			l.Cols[1] = -1
		} else {
			l.Cols[1] = v
		}
		return l

	}
	var wtr *bytes.Buffer

	err := gsort.Sort(data, wtr, pp, 22)
	c.Assert(err, IsNil)

	c.Assert(wtr.String(), Equals, "xx")

}
