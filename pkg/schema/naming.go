package schema

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

var (
	// https://github.com/golang/lint/blob/master/lint.go#L770
	commonInitialisms         = []string{"API", "ASCII", "CPU", "CSS", "DNS", "EOF", "GUID", "HTML", "HTTP", "HTTPS", "ID", "IP", "JSON", "LHS", "QPS", "RAM", "RHS", "RPC", "SLA", "SMTP", "SSH", "TLS", "TTL", "UID", "UI", "UUID", "URI", "URL", "UTF8", "VM", "XML", "XSRF", "XSS"}
	commonInitialismsReplacer *strings.Replacer
	_                         Namer = NamingStrategy{}
)

func init() {
	commonInitialismsForReplacer := make([]string, 0, len(commonInitialisms))
	for _, initialism := range commonInitialisms {
		commonInitialismsForReplacer = append(commonInitialismsForReplacer, initialism, strings.Title(strings.ToLower(initialism)))
	}
	commonInitialismsReplacer = strings.NewReplacer(commonInitialismsForReplacer...)
}

type Namer interface {
	TableName(table string) string
	SchemaName(table string) string
	ColumnName(table, column string) string
	IndexName(table, column string) string
}

type Replacer interface {
	Replace(name string) string
}

type NamingStrategy struct {
	TablePrefix  string
	NameReplacer Replacer
}

func (ns NamingStrategy) TableName(str string) string {
	return ns.TablePrefix + ns.toDBName(str)
}

func (ns NamingStrategy) SchemaName(table string) string {
	table = strings.TrimPrefix(table, ns.TablePrefix)
	result := strings.Replace(strings.Title(strings.Replace(table, "_", " ", -1)), " ", "", -1)
	for _, initialsm := range commonInitialisms {
		result = regexp.MustCompile(strings.Title(strings.ToLower(initialsm))+"([A-Z]|$|_").ReplaceAllString(result, initialsm+"$1")
	}
	return result
}

func (ns NamingStrategy) toDBName(name string) string {
	if name == "" {
		return ""
	}

	if ns.NameReplacer != nil {
		name = ns.NameReplacer.Replace(name)
	}

	var (
		value                          = commonInitialismsReplacer.Replace(name)
		buf                            strings.Builder
		lastCase, nextCase, nextNumber bool
		curCase                        = value[0] <= 'Z' && value[0] >= 'A'
	)

	for i, v := range value[:len(value)-1] {
		nextCase = value[i+1] <= 'Z' && value[i+1] >= 'A'
		nextNumber = value[i+1] >= '0' && value[i+1] <= '9'

		if curCase {
			if lastCase && (nextCase || nextNumber) {
				buf.WriteRune(v + 32)
			} else {
				if i > 0 && value[i-1] != '_' && value[i+1] != '_' {
					buf.WriteByte('_')
				}
				buf.WriteRune(v + 32)
			}
		} else {
			buf.WriteRune(v)
		}

		lastCase = curCase
		curCase = nextCase

	}

	if curCase {
		if !lastCase && len(value) > 1 {
			buf.WriteByte('_')
		}
		buf.WriteByte(value[len(value)-1] + 32)
	} else {
		buf.WriteByte(value[len(value)-1])
	}
	ret := buf.String()
	return ret
}

func (ns NamingStrategy) ColumnName(table, column string) string {
	return ns.toDBName(column)
}

func (ns NamingStrategy) IndexName(table, column string) string {
	return ns.formatName("idx", table, ns.toDBName(column))
}

func (ns NamingStrategy) formatName(prefix, table, name string) string {
	formattedName := strings.Replace(strings.Join([]string{
		prefix, table, name,
	}, "_"), ".", "_", -1)

	if utf8.RuneCountInString(formattedName) > 64 {
		h := sha1.New()
		h.Write([]byte(formattedName))
		bs := h.Sum(nil)

		formattedName = fmt.Sprintf("%v%v%v", prefix, table, name)[0:56] + hex.EncodeToString(bs)[:8]
	}
	return formattedName
}
