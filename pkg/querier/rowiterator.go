package querier

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"time"

	"github.com/grafana/loki/pkg/logproto"
	prometheustranslator "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus"
	"github.com/prometheus/prometheus/model/labels"
)

type rowEntryIterator struct {
	rows       *sql.Rows
	currentRow func() (serializedRow, error)
}

func (r *rowEntryIterator) readFromDB() (s serializedRow, err error) {
	labelMap := make(map[string]string)
	if err := r.rows.Scan(&s.timestamp, &s.body, &labelMap); err != nil {
		return serializedRow{}, fmt.Errorf("failed to scan row: %w", err)
	}

	s.labelMap = make(map[string]string, len(labelMap))
	for k, v := range labelMap {
		s.labelMap[prometheustranslator.NormalizeLabel(k)] = v
	}
	s.labels = labels.FromMap(s.labelMap)

	return s, nil
}

func (r *rowEntryIterator) Next() bool {
	next := r.rows.Next()
	if next {
		// reset r.currentRow state
		r.currentRow = sync.OnceValues(r.readFromDB)

		// we trigger the read on the Next call.
		// If an error occurs we return early
		if _, err := r.currentRow(); err != nil {
			log.Printf("failed to read row: %s", err)
			return false
		}
	}

	return next
}

func (r *rowEntryIterator) Labels() string {
	row, err := r.currentRow()
	if err != nil {
		return ""
	}

	return row.labels.String()
}

func (r *rowEntryIterator) StreamHash() uint64 {
	return hashLabels(r.Labels())
}

func hashLabels(lbs string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(lbs))
	return h.Sum64()
}

func (r *rowEntryIterator) Error() error {
	err := r.rows.Err()
	if err == nil && r.currentRow != nil {
		_, err = r.currentRow()
	}

	return err
}

func (r *rowEntryIterator) Close() error {
	return r.rows.Close()
}

type serializedRow struct {
	// read from db
	timestamp time.Time
	body      string
	labelMap  map[string]string

	// serialized based on map
	labels labels.Labels
}

func (r *rowEntryIterator) Entry() logproto.Entry {
	row, err := r.currentRow()
	if err != nil {
		return logproto.Entry{}
	}

	return logproto.Entry{
		Timestamp: row.timestamp,
		Line:      row.body,
	}
}
