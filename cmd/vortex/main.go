package main

import (
	"crypto/tls"
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	kitlog "github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/loki/pkg/querier"
	"github.com/grafana/loki/pkg/util/server"
	"github.com/grafana/loki/pkg/validation"
	"github.com/weaveworks/common/user"

	chquerier "github.com/monogon-dev/vortex/pkg/querier"
)

func main() {
	var (
		clickhouseAddr     = flag.String("clickhouse.addr", "clickhouse.lgtm-0.internal.api.openai.org:443", "Clickhouse address")
		clickhouseDatabase = flag.String("clickhouse.database", "otel", "Clickhouse database")
		clickhouseTable    = flag.String("clickhouse.table", "logs", "Clickhouse table")
		clickhouseUsername = flag.String("clickhouse.username", "default", "Clickhouse username")
		clickhousePassword = flag.String("clickhouse.password", "", "Clickhouse password")
	)
	flag.Parse()

	logger := kitlog.NewLogfmtLogger(os.Stderr)
	log.SetOutput(kitlog.NewStdlibAdapter(logger))

	conn := clickhouse.OpenDB(&clickhouse.Options{
		// TODO(rbtz): Compression
		// TODO(rbtz): Native protocol
		Protocol: clickhouse.HTTP,
		Addr:     []string{*clickhouseAddr},
		TLS:      &tls.Config{},
		Auth: clickhouse.Auth{
			Database: *clickhouseDatabase,
			Username: *clickhouseUsername,
			Password: *clickhousePassword,
		},
	})

	if err := conn.Ping(); err != nil {
		log.Fatalf("connect failed: %s", err)
	}

	defaultLimits := validation.Limits{}
	flagext.DefaultValues(&defaultLimits)
	limits, err := validation.NewOverrides(defaultLimits, nil)
	if err != nil {
		log.Fatalf("limits failed: %s", err)
	}

	chq := chquerier.NewClickhouseQuerier(conn, *clickhouseDatabase, *clickhouseTable)
	api := querier.NewQuerierAPI(querier.Config{}, chq, limits, logger)

	routes := map[string]http.Handler{
		"/loki/api/v1/query_range": querier.WrapQuerySpanAndTimeout("query.RangeQuery", api).Wrap(http.HandlerFunc(api.RangeQueryHandler)),
		"/loki/api/v1/query":       querier.WrapQuerySpanAndTimeout("query.InstantQuery", api).Wrap(http.HandlerFunc(api.InstantQueryHandler)),

		"/loki/api/v1/label":               http.HandlerFunc(api.LabelHandler),
		"/loki/api/v1/labels":              http.HandlerFunc(api.LabelHandler),
		"/loki/api/v1/label/{name}/values": http.HandlerFunc(api.LabelHandler),

		"/loki/api/v1/series":              querier.WrapQuerySpanAndTimeout("query.Series", api).Wrap(http.HandlerFunc(api.SeriesHandler)),
		"/loki/api/v1/index/stats":         querier.WrapQuerySpanAndTimeout("query.IndexStats", api).Wrap(http.HandlerFunc(api.IndexStatsHandler)),
		"/loki/api/v1/index/series_volume": querier.WrapQuerySpanAndTimeout("query.SeriesVolume", api).Wrap(http.HandlerFunc(api.SeriesVolumeHandler)),
	}

	router := mux.NewRouter()
	for path, handler := range routes {
		path, handler := path, handler
		router.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			newCtx := user.InjectOrgID(r.Context(), "0")
			if err := r.ParseForm(); err != nil {
				server.WriteError(err, w)
				return
			}
			handler.ServeHTTP(w, r.WithContext(newCtx))
		})
	}

	router.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println(r.URL)
		http.NotFoundHandler().ServeHTTP(w, r)
	})

	log.Println("Started")
	log.Fatal(http.ListenAndServe(":3100", router))
}
