package parser

import "regexp"

var (
	SqlTypeExp = regexp.MustCompile(`(SELECT)|(UPDATE)|(INSERT)`)
)
