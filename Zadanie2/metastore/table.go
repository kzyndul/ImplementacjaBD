package metastore

import (
	"fmt"
	"sync"
	"time"
)

// Table holds table schema and data files metadata.
type Table struct {
	ID            string              `json:"id"`
	Name          string              `json:"name"`
	Columns       []Column            `json:"columns"`
	ColumnMapping map[string]int      `json:"columnsMapping"`
	DataFiles     map[string]DataFile `json:"data_files"`
	CreatedAt     time.Time           `json:"created_at"`
	LastModified  time.Time           `json:"last_modified"`
	lock          sync.RWMutex        `json:"-"`
}

func (t *Table) AcquireRead()  { t.lock.RLock() }
func (t *Table) ReleaseRead()  { t.lock.RUnlock() }
func (t *Table) AcquireWrite() { t.lock.Lock() }
func (t *Table) ReleaseWrite() { t.lock.Unlock() }

func (t *Table) AddDataFile(column string, filePath string) {
	t.AcquireWrite()
	defer t.ReleaseWrite()

	_, ok := t.ColumnMapping[column]
	if !ok {
		return
	}
	t.DataFiles[column] = DataFile{Path: filePath, CreatedAt: time.Now()}
	t.LastModified = time.Now()
}

func (t *Table) GetDataFiles() []string {
	// t.AcquireRead()
	// defer t.ReleaseRead()

	out := make([]string, len(t.DataFiles))
	for _, df := range t.DataFiles {
		out = append(out, df.Path)
	}
	return out
}

func (t *Table) ColumnPath(name string) (string, error) {
	// t.AcquireRead()
	// defer t.ReleaseRead()

	idx, ok := t.ColumnMapping[name]
	if !ok {
		return "", fmt.Errorf("column '%s' does not exist", name)
	}
	return t.Columns[idx].Path, nil
}
