package ql

import (
	"fmt"
	"log"
	"strings"

	"github.com/grafana/loki/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"
)

type logQLTransformer struct {
	*selectBuilder
}

func denormalizeLabel(label string) string {
	return strings.Map(func(r rune) rune {
		if r == '_' {
			return '.'
		}
		return r
	}, label)
}

func (b *logQLTransformer) AcceptMatcher(m *labels.Matcher) {
	if m.Value == "" && m.GetRegexString() == "" {
		return
	}

	keySelector := fmt.Sprintf(
		"arrayElement(`%s`, %s)",
		OtelLogLabelsColumn,
		b.Args.Add(denormalizeLabel(m.Name)),
	)
	switch m.Type {
	case labels.MatchEqual:
		b.Where(b.EqualField(keySelector, m.Value))
	case labels.MatchNotEqual:
		b.Where(b.NotEqualField(keySelector, m.Value))
	case labels.MatchRegexp:
		b.Where(b.MatchField(keySelector, m.GetRegexString()))
	case labels.MatchNotRegexp:
		b.Where(b.NotMatchField(keySelector, m.GetRegexString()))
	default:
		panic(fmt.Sprintf("invalid match type: %v", m.Type))
	}
}

func (b *logQLTransformer) AcceptPipelineExpr(expr *syntax.PipelineExpr) {
	for _, matcher := range expr.Matchers() {
		b.AcceptMatcher(matcher)
	}
}

func (b *logQLTransformer) AcceptMatchersExpr(expr *syntax.MatchersExpr) {
	for _, matcher := range expr.Matchers() {
		b.AcceptMatcher(matcher)
	}
}

func (b *logQLTransformer) AcceptLineFilterExpr(expr *syntax.LineFilterExpr) {
	if expr.Match == "" {
		return
	}

	switch expr.Ty {
	case labels.MatchEqual:
		b.Where(b.Like(OtelLogMessageColumn, "%"+expr.Match+"%"))
	case labels.MatchNotEqual:
		b.Where(b.NotLike(OtelLogMessageColumn, "%"+expr.Match+"%"))
	case labels.MatchRegexp:
		b.Where(b.Match(OtelLogMessageColumn, expr.Match))
	case labels.MatchNotRegexp:
		b.Where(b.NotMatch(OtelLogMessageColumn, expr.Match))
	default:
		log.Printf("AcceptLineFilterExpr: invalid match type: %v", expr.Ty)
	}
}

func (b *logQLTransformer) AcceptLogSelectorExpr(expr syntax.LogSelectorExpr) {
	expr.Walk(func(e any) {
		switch e.(type) {
		case *syntax.PipelineExpr:
			log.Printf("AcceptLogSelectorExpr: PipelineExpr: %#v", e)
		case *syntax.MatchersExpr:
			b.AcceptMatchersExpr(e.(*syntax.MatchersExpr))
		case *syntax.LineFilterExpr:
			b.AcceptLineFilterExpr(e.(*syntax.LineFilterExpr))
		default:
			log.Printf("AcceptLogSelectorExpr: %#v", e)
		}
	})
}
