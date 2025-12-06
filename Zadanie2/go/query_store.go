package openapi

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

type internalQuery struct {
	ID              string
	QueryDefinition QueryQueryDefinition

	// Immutable fields (set at creation, never modified)
	IsSelect  bool
	IsDelete  bool
	Submitted time.Time

	// Mutable fields (protected by mu)
	Status            QueryStatus
	IsResultAvailable bool
	Started           *time.Time
	Finished          *time.Time
	Error             *MultipleProblemsError
	ResultRows        QueryResultInner

	doneChan chan struct{}
	mu       sync.RWMutex
}

// Thread-safe getters
func (iq *internalQuery) GetStatus() QueryStatus {
	iq.mu.RLock()
	defer iq.mu.RUnlock()
	return iq.Status
}

func (iq *internalQuery) GetIsResultAvailable() bool {
	iq.mu.RLock()
	defer iq.mu.RUnlock()
	return iq.IsResultAvailable
}

func (iq *internalQuery) GetResultRows() QueryResultInner {
	iq.mu.RLock()
	defer iq.mu.RUnlock()
	return iq.ResultRows
}

func (iq *internalQuery) GetError() *MultipleProblemsError {
	iq.mu.RLock()
	defer iq.mu.RUnlock()
	return iq.Error
}

// Thread-safe setters
func (iq *internalQuery) SetRunning(started time.Time) {
	iq.mu.Lock()
	defer iq.mu.Unlock()
	iq.Status = RUNNING
	iq.Started = &started
}

func (iq *internalQuery) SetCompleted(finished time.Time, rows QueryResultInner, isResultAvailable bool) {
	iq.mu.Lock()
	defer iq.mu.Unlock()
	iq.Status = COMPLETED
	iq.Finished = &finished
	iq.ResultRows = rows
	iq.IsResultAvailable = isResultAvailable
}

func (iq *internalQuery) SetFailed(finished time.Time, err *MultipleProblemsError) {
	iq.mu.Lock()
	defer iq.mu.Unlock()
	iq.Status = FAILED
	iq.Finished = &finished
	iq.Error = err
}

func (iq *internalQuery) ClearResult() {
	iq.mu.Lock()
	defer iq.mu.Unlock()
	iq.IsResultAvailable = false
	iq.ResultRows = QueryResultInner{}
}

type queryStore struct {
	mu      sync.RWMutex
	queries map[string]*internalQuery
}

func (query QueryQueryDefinition) string() string {
	var sb strings.Builder
	sb.WriteString("[Table=" + query.TableName)
	sb.WriteString(", DestinationColumns=[" + strings.Join(query.DestinationColumns, ", ") + "]")
	sb.WriteString(", SourceFilepath=" + query.SourceFilepath)
	sb.WriteString(", DestinationTableName=" + query.DestinationTableName)
	sb.WriteString("]")
	return sb.String()
}

func (iq *internalQuery) string() string {
	status := iq.GetStatus()
	return "Query[ID=" + iq.ID + ", Status=" + string(status) + iq.QueryDefinition.string() +
		" isSelect=" + strconv.FormatBool(iq.IsSelect) + ", isDelete=" + strconv.FormatBool(iq.IsDelete) + "]"
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
		Status:  q.GetStatus(),
	}
}

func toPublic(q *internalQuery) Query {
	return Query{
		QueryId:           q.ID,
		Status:            q.GetStatus(),
		IsResultAvailable: q.GetIsResultAvailable(),
		QueryDefinition:   q.QueryDefinition,
	}
}
