package metastore

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type ColumnType byte

const (
	TypeInt    ColumnType = 0
	TypeString ColumnType = 1
)

type Column struct {
	Name string     `json:"name"`
	Path string     `json:"path"`
	Type ColumnType `json:"type"`
}

type DataFile struct {
	Path      string    `json:"path"`
	CreatedAt time.Time `json:"created_at"`
}

type Metastore struct {
	Tables        map[string]*Table `json:"tables"`
	mu            sync.RWMutex
	metastorePath string
}

func NewMetastore(metastorePath string) *Metastore {
	return &Metastore{
		Tables:        make(map[string]*Table),
		metastorePath: metastorePath,
	}
}

func (ct ColumnType) String() string {
	switch ct {
	case TypeInt:
		return "int"
	case TypeString:
		return "string"
	default:
		return fmt.Sprintf("unknown(%d)", ct)
	}
}

// TODO
func (m *Metastore) GetDataDir() string {
	return filepath.Join(m.metastorePath, "data")
}

func validateColumns(columns []Column) error {
	seen := make(map[string]bool)
	for _, col := range columns {
		if col.Name == "" {
			return fmt.Errorf("empty column name")
		}
		if seen[col.Name] {
			return fmt.Errorf("duplicate column name: %s", col.Name)
		}
		seen[col.Name] = true
		if col.Type != TypeInt && col.Type != TypeString {
			return fmt.Errorf("invalid type for %s", col.Name)
		}
	}
	return nil
}

func validateTableName(name string) error {
	if name == "" {
		return fmt.Errorf("empty table name")
	}
	if strings.ContainsAny(name, "/\\:*?\"<>|") {
		return fmt.Errorf("invalid characters in table name")
	}
	return nil
}

func (m *Metastore) DebugMetadata() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var b strings.Builder
	b.WriteString("Metastore:\n")
	if len(m.Tables) == 0 {
		b.WriteString("  (no tables)\n")
		return b.String()
	}

	// Stable order
	names := make([]string, 0, len(m.Tables))
	for name := range m.Tables {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		t := m.Tables[name]
		b.WriteString(fmt.Sprintf("Table: %s\n", t.Name))
		b.WriteString(fmt.Sprintf("  Created: %s  LastModified: %s\n", t.CreatedAt.Format(time.RFC3339), t.LastModified.Format(time.RFC3339)))

		// Columns
		b.WriteString("  Columns:\n")
		if len(t.Columns) == 0 {
			b.WriteString("    (no columns)\n")
		} else {
			// print deterministically
			keys := make([]string, 0, len(t.ColumnMapping))
			for key, _ := range t.ColumnMapping {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for idx, key := range keys {
				col := t.Columns[t.ColumnMapping[key]]
				b.WriteString(fmt.Sprintf("    [%d] %s (type=%s)\n", idx, col.Name, col.Type.String()))
			}
		}

		// Data files
		b.WriteString("  DataFiles:\n")
		if len(t.DataFiles) == 0 {
			b.WriteString("    (no data files)\n")
		} else {
			keys := make([]string, 0, len(t.DataFiles))
			for k := range t.DataFiles {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for idx, key := range keys {
				df := t.DataFiles[key]
				b.WriteString(fmt.Sprintf("    [%d] %s (created=%s)\n", idx, df.Path, df.CreatedAt.Format(time.RFC3339)))
			}
		}

		b.WriteString("\n")
	}
	return b.String()
}

func (m *Metastore) PrintMetadata(w io.Writer) {
	_, _ = io.WriteString(w, m.DebugMetadata())
}

// Load from JSON file
func (m *Metastore) Load() error {
	data, err := os.ReadFile(m.metastorePath)
	if err != nil {
		if os.IsNotExist(err) {
			m.Tables = make(map[string]*Table)
			return nil
		}
		return fmt.Errorf("read error: %w", err)
	}
	if err := json.Unmarshal(data, m); err != nil {
		return fmt.Errorf("parse error: %w", err)
	}
	if m.Tables == nil {
		m.Tables = make(map[string]*Table)
	}
	return nil
}

func (m *Metastore) Save() error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("serialize error: %w", err)
	}
	tmp := m.metastorePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write error: %w", err)
	}
	if err := os.Rename(tmp, m.metastorePath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename error: %w", err)
	}
	return nil
}

func (m *Metastore) CreateTable(name string, columns []Column, dataDir string) (string, error) {
	if err := validateTableName(name); err != nil {
		return "", err
	}

	if len(columns) == 0 {
		return "", fmt.Errorf("can't create table with zero columns")
	}
	if err := validateColumns(columns); err != nil {
		return "", err
	}
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.Tables[name]; exists {
		return "", fmt.Errorf("table %s already exists", name)
	}

	t := &Table{
		ID:            uuid.NewString(),
		Name:          name,
		Columns:       make([]Column, len(columns)),
		ColumnMapping: make(map[string]int),
		DataFiles:     make(map[string]DataFile),
		CreatedAt:     now,
		LastModified:  now,
	}

	err := os.MkdirAll(filepath.Join(dataDir, name), 0755)
	if err != nil {
		return "", fmt.Errorf("failed to create table directory: %w", err)
	}

	// set column paths based on dataDir
	// TODO
	for idx, col := range columns {
		if col.Path == "" && dataDir != "" {
			col.Path = filepath.Join(dataDir, name, col.Name)
		}
		t.Columns[idx] = col
		t.ColumnMapping[col.Name] = idx
	}

	m.Tables[name] = t
	return t.ID, nil
}

func (m *Metastore) GetTableById(tableID string) (*Table, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, table := range m.Tables {
		if table.ID == tableID {
			return table, nil
		}
	}
	return nil, fmt.Errorf("couldn't find a table with ID: %s", tableID)
}

func (m *Metastore) GetTableByName(name string) (*Table, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getTable(name)
}

func (m *Metastore) getTable(name string) (*Table, error) {
	t, ok := m.Tables[name]
	if !ok {
		return nil, fmt.Errorf("couldn't find a table of given ID: %s", name)
	}
	return t, nil
}

func (m *Metastore) DropTable(tableName string) error {
	m.mu.Lock()
	table, err := m.getTable(tableName)
	if err != nil {
		m.mu.Unlock()
		return err
	}

	table.AcquireWrite()
	defer table.ReleaseWrite()
	tableFiles := table.GetDataFiles()
	// log.Println("Deleting table", tableName, "with files:", tableFiles)

	delete(m.Tables, tableName)
	m.mu.Unlock()

	// log.Println("Deleting table2", tableName, "with files:", tableFiles)

	for _, filePath := range tableFiles {
		if err := os.Remove(filePath); err != nil {
			return fmt.Errorf("failed to delete data file %s: %w", filePath, err)
		}
	}

	return nil
}

func (m *Metastore) ListTables() []*Table {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Table, 0, len(m.Tables))
	for _, v := range m.Tables {
		out = append(out, v)
	}
	return out
}
