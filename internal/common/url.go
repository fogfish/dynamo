package common

import (
	"net/url"
	"strings"
)

/*

URL custom type with helper functions
*/
type URL url.URL

// query parameters
func (uri *URL) Query(key, def string) string {
	val := (*url.URL)(uri).Query().Get(key)

	if val == "" {
		return def
	}

	return val
}

// path segments of length
func (uri *URL) Segments(n int) []*string {
	seq := make([]*string, n)

	seg := strings.Split((*url.URL)(uri).Path, "/")[1:]
	for i, x := range seg {
		val := x
		seq[i] = &val
	}

	return seq
}
