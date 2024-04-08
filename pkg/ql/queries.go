package ql

import (
	"fmt"
	"time"

	"github.com/grafana/loki/pkg/logproto"
	"github.com/grafana/loki/pkg/logql/syntax"
	"github.com/huandu/go-sqlbuilder"
	"github.com/prometheus/prometheus/model/labels"
)

const (
	OtelLogLabelsColumn    = "ResourceAttributes"
	OtelLogTimestampColumn = "Timestamp"
	OtelLogMessageColumn   = "Body"
)

type otelEnvironmentImpl struct {
	dbName        string
	otelLogsTable string
}

type Environment interface {
	SelectLogsQuery(expr syntax.LogSelectorExpr, start time.Time, end time.Time, limit uint32, direction logproto.Direction) (string, []any)
	LabelQuery(name string, values bool, start *time.Time, end *time.Time) (string, []any)
	SeriesQuery(groups [][]*labels.Matcher, start time.Time, end time.Time) (string, []any)
}

func NewOtelEnvironment(
	dbName string,
	otelLogsTable string,
) *otelEnvironmentImpl {
	return &otelEnvironmentImpl{
		dbName:        dbName,
		otelLogsTable: otelLogsTable,
	}
}

func (o *otelEnvironmentImpl) tableName() string {
	return fmt.Sprintf("`%s`.`%s`", o.dbName, o.otelLogsTable)
}

func (o *otelEnvironmentImpl) SelectLogsQuery(expr syntax.LogSelectorExpr, start time.Time, end time.Time, limit uint32, direction logproto.Direction) (string, []any) {
	sb := &selectBuilder{sqlbuilder.ClickHouse.NewSelectBuilder()}
	sb.Select(
		fmt.Sprintf("`%s`", OtelLogTimestampColumn),
		fmt.Sprintf("`%s`", OtelLogMessageColumn),
		fmt.Sprintf("`%s`", OtelLogLabelsColumn),
	).From(o.tableName())

	sb.Where(fmt.Sprintf("`%s` >= fromUnixTimestamp64Milli(%d)", OtelLogTimestampColumn, start.UnixMilli()))
	sb.Where(fmt.Sprintf("`%s` <= fromUnixTimestamp64Milli(%d)", OtelLogTimestampColumn, end.UnixMilli()))
	sb.Limit(int(limit))

	orderBy := fmt.Sprintf("`%s`", OtelLogTimestampColumn)
	switch direction {
	case logproto.BACKWARD:
		orderBy += " DESC"
	case logproto.FORWARD:
		orderBy += " ASC"
	}
	sb.OrderBy(orderBy)

	l := &logQLTransformer{sb}
	l.AcceptLogSelectorExpr(expr)

	return sb.Build()
}

func (o *otelEnvironmentImpl) LabelQuery(name string, values bool, start *time.Time, end *time.Time) (string, []any) {
	sb := &selectBuilder{sqlbuilder.ClickHouse.NewSelectBuilder()}
	sb.From(o.tableName()).Distinct()

	if values {
		name = denormalizeLabel(name)
		sb.Select(fmt.Sprintf("arrayElement(`%s`, %s)", OtelLogLabelsColumn, sb.Args.Add(name)))
	} else {
		sb.Select(fmt.Sprintf("arrayJoin(mapKeys(`%s`))", OtelLogLabelsColumn))
	}
	if start != nil {
		sb.Where(fmt.Sprintf("`%s` >= fromUnixTimestamp64Milli(%d)", OtelLogTimestampColumn, start.UnixMilli()))
	}
	if end != nil {
		sb.Where(fmt.Sprintf("`%s` <= fromUnixTimestamp64Milli(%d)", OtelLogTimestampColumn, end.UnixMilli()))
	}

	return sb.Build()
}

func (o *otelEnvironmentImpl) SeriesQuery(groups [][]*labels.Matcher, start time.Time, end time.Time) (string, []any) {
	sb := &selectBuilder{sqlbuilder.ClickHouse.NewSelectBuilder()}
	sb.From(o.tableName()).Distinct()

	sb.Select(fmt.Sprintf("`%s`", OtelLogLabelsColumn))
	sb.Where(fmt.Sprintf("`%s` >= fromUnixTimestamp64Milli(%d)", OtelLogTimestampColumn, start.UnixMilli()))
	sb.Where(fmt.Sprintf("`%s` <= fromUnixTimestamp64Milli(%d)", OtelLogTimestampColumn, end.UnixMilli()))

	l := &logQLTransformer{sb}

	for _, group := range groups {
		for _, matcher := range group {
			l.AcceptMatcher(matcher)
		}
	}

	return l.Build()
}
