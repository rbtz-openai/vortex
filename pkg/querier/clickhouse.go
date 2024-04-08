package querier

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/grafana/loki/pkg/iter"
	"github.com/grafana/loki/pkg/loghttp"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/grafana/loki/pkg/logql"
	"github.com/grafana/loki/pkg/logqlmodel/stats"
	"github.com/grafana/loki/pkg/querier"
	indexStats "github.com/grafana/loki/pkg/storage/stores/index/stats"
	prometheustranslator "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus"

	"github.com/rbtz-openai/vortex/pkg/ql"
)

func NewClickhouseQuerier(
	db *sql.DB,
	databaseName string,
	tableName string,
) querier.Querier {
	return &clickhouseQuerier{
		conn: db,
		env:  ql.NewOtelEnvironment(databaseName, tableName),
	}
}

type clickhouseQuerier struct {
	conn *sql.DB
	env  ql.Environment
}

func (cq *clickhouseQuerier) Label(ctx context.Context, req *logproto.LabelRequest) (*logproto.LabelResponse, error) {
	query, args := cq.env.LabelQuery(req.Name, req.Values, req.Start, req.End)

	statsCtx := stats.FromContext(ctx)
	chCtx := clickhouse.Context(ctx, clickhouse.WithProfileInfo(func(info *clickhouse.ProfileInfo) {
		statsCtx.AddDecompressedLines(int64(info.Rows))
		statsCtx.AddDecompressedBytes(int64(info.Bytes))
	}))

	rows, err := cq.conn.QueryContext(chCtx, query, args...)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()

	var lr logproto.LabelResponse
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		value = prometheustranslator.NormalizeLabel(value)

		lr.Values = append(lr.Values, value)
	}

	return &lr, nil
}

func (cq *clickhouseQuerier) Series(ctx context.Context, req *logproto.SeriesRequest) (*logproto.SeriesResponse, error) {
	matcherGroups, err := logql.Match(req.Groups)
	if err != nil {
		return nil, err
	}

	query, args := cq.env.SeriesQuery(matcherGroups, req.Start, req.End)

	statsCtx := stats.FromContext(ctx)
	chCtx := clickhouse.Context(ctx, clickhouse.WithProfileInfo(func(info *clickhouse.ProfileInfo) {
		statsCtx.AddDecompressedLines(int64(info.Rows))
		statsCtx.AddDecompressedBytes(int64(info.Bytes))
	}))

	rows, err := cq.conn.QueryContext(chCtx, query, args...)
	if err != nil {
		log.Println(err)
		return nil, fmt.Errorf("failed to query: %w", err)
	}
	defer rows.Close()

	var lr logproto.SeriesResponse
	for rows.Next() {
		m := make(map[string]string)
		if err := rows.Scan(&m); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		normalized := make(map[string]string, len(m))
		for key, value := range m {
			normalized[prometheustranslator.NormalizeLabel(key)] = value
		}

		lr.Series = append(lr.Series, logproto.SeriesIdentifier{Labels: normalized})
	}

	return &lr, nil
}

func (cq *clickhouseQuerier) Tail(ctx context.Context, req *logproto.TailRequest) (*querier.Tailer, error) {
	log.Println("Tail")
	return nil, fmt.Errorf("Tail: not implemented")
}

func (cq *clickhouseQuerier) IndexStats(ctx context.Context, req *loghttp.RangeQuery) (*indexStats.Stats, error) {
	log.Println("IndexStats")
	return nil, fmt.Errorf("IndexStats: not implemented")
}

func (cq *clickhouseQuerier) SeriesVolume(ctx context.Context, req *logproto.VolumeRequest) (*logproto.VolumeResponse, error) {
	log.Println("SeriesVolume")
	return nil, fmt.Errorf("SeriesVolume: not implemented")
}

func (cq *clickhouseQuerier) SelectLogs(ctx context.Context, params logql.SelectLogParams) (iter.EntryIterator, error) {
	selector, err := params.LogSelector()
	if err != nil {
		return nil, err
	}

	query, args := cq.env.SelectLogsQuery(selector, params.Start, params.End, params.Limit, params.Direction)

	statsCtx := stats.FromContext(ctx)
	chCtx := clickhouse.Context(ctx, clickhouse.WithProfileInfo(func(info *clickhouse.ProfileInfo) {
		statsCtx.AddDecompressedLines(int64(info.Rows))
		statsCtx.AddDecompressedBytes(int64(info.Bytes))
	}))

	rows, err := cq.conn.QueryContext(chCtx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query: %w", err)
	}

	return &rowEntryIterator{rows: rows}, nil
}

func (cq *clickhouseQuerier) SelectSamples(ctx context.Context, params logql.SelectSampleParams) (iter.SampleIterator, error) {
	log.Println("SelectSamples")
	return nil, fmt.Errorf("SelectSamples: not implemented")
}
