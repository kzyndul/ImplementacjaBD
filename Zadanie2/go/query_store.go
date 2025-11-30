package openapi

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

type internalQuery struct {
	ID                string
	QueryDefinition   QueryQueryDefinition
	Status            QueryStatus
	IsResultAvailable bool

	IsSelect bool
	IsDelete bool

	Submitted  time.Time
	Started    *time.Time
	Finished   *time.Time
	Error      *MultipleProblemsError // TODO what to do here
	ResultRows QueryResultInner       // TODO what to do here
}

type queryStore struct {
	mu      sync.RWMutex
	queries map[string]*internalQuery
}

func (query QueryQueryDefinition) string() string {
	var sb strings.Builder
	sb.WriteString("[Table=" + query.TableName)
	sb.WriteString(", Type=DestinationColumns=[" + strings.Join(query.DestinationColumns, ", ") + "]")
	sb.WriteString(", Type=SourceFilepath=" + query.SourceFilepath)
	sb.WriteString(", Type=LOAD, DestinationColumns=[" + strings.Join(query.DestinationColumns, ", ") + "]")
	sb.WriteString(", Type=DestinationTableName=" + query.DestinationTableName)
	sb.WriteString("]")
	return sb.String()
}

func (iq internalQuery) string() string {
	return "Query[ID=" + iq.ID + ", Status=" + string(iq.Status) + iq.QueryDefinition.string() + " isSelect=" + strconv.FormatBool(iq.IsSelect) + ", isDelete=" + strconv.FormatBool(iq.IsDelete) + "]"
}

func newQueryStore() *queryStore {
	return &queryStore{queries: make(map[string]*internalQuery)}
}

func (qs *queryStore) add(q *internalQuery) {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	qs.queries[q.ID] = q
}

func (qs *queryStore) get(id string) (*internalQuery, bool) {
	qs.mu.RLock()
	defer qs.mu.RUnlock()
	q, ok := qs.queries[id]
	return q, ok
}

func (qs *queryStore) list() []*internalQuery {
	qs.mu.RLock()
	defer qs.mu.RUnlock()
	out := make([]*internalQuery, 0, len(qs.queries))
	for _, q := range qs.queries {
		out = append(out, q)
	}
	return out
}

func toShallow(q *internalQuery) ShallowQuery {
	return ShallowQuery{
		QueryId: q.ID,
		Status:  q.Status,
	}
}

func toPublic(q *internalQuery) Query {
	return Query{
		QueryId:           q.ID,
		Status:            q.Status,
		IsResultAvailable: q.IsResultAvailable,
		QueryDefinition:   q.QueryDefinition,
	}
}
