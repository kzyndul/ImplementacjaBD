package openapi

import (
	"Zadanie2/deserializer"
	"Zadanie2/metastore"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type QueryScheduler struct {
	ms         *metastore.Metastore
	qs         *queryStore
	workQueue  chan string
	stopChan   chan struct{}
	numWorkers int
	wg         sync.WaitGroup
	dataDir    string
}

// TODO
func NewQueryScheduler(ms *metastore.Metastore, qs *queryStore, numWorkers int, dataDir string) *QueryScheduler {
	return &QueryScheduler{
		ms:         ms,
		qs:         qs,
		workQueue:  make(chan string, 100),
		stopChan:   make(chan struct{}),
		numWorkers: numWorkers,
		dataDir:    dataDir,
	}
}

func (sched *QueryScheduler) Start() {
	for i := 0; i < sched.numWorkers; i++ {
		sched.wg.Add(1)
		go sched.worker(i)
	}
	log.Printf("Query scheduler started with %d workers", sched.numWorkers)
}

func (sched *QueryScheduler) Stop() {
	close(sched.stopChan)
	sched.wg.Wait()
	log.Println("Query scheduler stopped")
}

func (sched *QueryScheduler) SubmitQuery(queryID string) {
	select {
	case sched.workQueue <- queryID:
		log.Printf("Query %s submitted to scheduler", queryID)
	case <-sched.stopChan:
		log.Printf("Scheduler stopped, cannot submit query %s", queryID)
	}
}

func (sched *QueryScheduler) worker(id int) {
	defer sched.wg.Done()
	log.Printf("Worker %d started", id)

	for {
		select {
		case queryID := <-sched.workQueue:
			sched.executeQuery(id, queryID)
		case <-sched.stopChan:
			log.Printf("Worker %d stopped", id)
			return
		}
	}
}

// executeQuery executes a single query based on its type
func (sched *QueryScheduler) executeQuery(workerID int, queryID string) {
	iq, ok := sched.qs.get(queryID)
	log.Println("executeQuery", iq.string())
	if !ok {
		log.Printf("Worker %d: Query %s not found", workerID, queryID)
		return
	}

	log.Printf("Worker %d: Executing query %s (SELECT=%v)", workerID, queryID, iq.IsSelect)

	// Update status to RUNNING
	now := time.Now()
	iq.Status = RUNNING
	iq.Started = &now

	var err error
	if iq.IsDelete {
		// DELETE query
		err = sched.executeDelete(iq)
	} else if iq.IsSelect {
		// SELECT query
		err = sched.executeSelect(iq)
	} else {
		// LOAD query
		err = sched.executeLoad(iq)
	}

	// Update final status
	finished := time.Now()
	iq.Finished = &finished

	if err != nil {
		iq.Status = FAILED
		errMsg := err.Error()
		iq.Error = &MultipleProblemsError{
			Problems: []MultipleProblemsErrorProblemsInner{{Error: errMsg}},
		}
		log.Printf("Worker %d: Query %s FAILED: %v", workerID, queryID, err)
	} else {
		iq.Status = COMPLETED
		iq.IsResultAvailable = iq.IsSelect
		log.Printf("Worker %d: Query %s COMPLETED", workerID, queryID)
	}
}

// executeSelect handles SELECT query execution
func (sched *QueryScheduler) executeSelect(iq *internalQuery) error {

	tableName := iq.QueryDefinition.TableName

	// TODO check table existence erlier
	table, err := sched.ms.GetTableByName(tableName)
	if err != nil {
		return err
	}

	table.AcquireRead()
	defer table.ReleaseRead()

	tableDataFiles := table.GetDataFiles()

	rows, err := sched.readTableData(table, tableDataFiles, iq.QueryDefinition)
	if err != nil {
		return err
	}
	// table.ReleaseRead()

	iq.ResultRows = rows

	return nil
}

// readTableData reads data from table files and applies filtering/projection
func (sched *QueryScheduler) readTableData(table *metastore.Table, dataFiles []string, qd QueryQueryDefinition) (QueryResultInner, error) {

	allRows := QueryResultInner{}

	// for _, filePath := range dataFiles {
	des, _ := deserializer.NewBatchDeserializer(sched.dataDir)
	ints, strings, err := des.ReadTableData()
	if err != nil {
		return QueryResultInner{}, fmt.Errorf("failed to read file: %w", err)
	}

	allRows.RowCount = int32(len(ints[0]))
	allRows.Columns = make([]QueryResultInnerColumnsInner, len(ints))

	// Convert to QueryResultInner format

	for idx, row := range ints {
		strCol, ok := strings[idx]

		if ok {
			col := make([]interface{}, len(row)-1)
			// strs := make([]string, len(row)-1)
			for i := 0; i < len(row)-1; i++ {
				col[i] = strCol[row[i]:row[i+1]]
			}
			allRows.Columns[idx] = col

		} else {
			col := make([]interface{}, len(row))
			for i := 0; i < len(row); i++ {
				col[i] = row[i]
			}
			allRows.Columns[idx] = col
		}

	}
	return allRows, nil
}

// // readBatchFile reads a single batch file and converts to QueryResultInner format
// func (sched *QueryScheduler) readBatchFile(table *metastore.Table, filePath string) ([]QueryResultInner, error) {
// 	// TODO: Replace with your actual batch deserializer
// 	// For now, this is a placeholder that assumes CSV-like storage

// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer file.Close()

// 	// Placeholder: just return empty rows
// 	// You should integrate with your LoadAllBatches() function here
// 	log.Printf("Reading batch file: %s (placeholder implementation)", filePath)

// 	return []QueryResultInner{}, nil
// }

func (sched *QueryScheduler) executeLoad(iq *internalQuery) error {

	tableName := iq.QueryDefinition.DestinationTableName
	csvPath := iq.QueryDefinition.SourceFilepath

	destCols := iq.QueryDefinition.DestinationColumns
	header := iq.QueryDefinition.DoesCsvContainHeader

	table, err := sched.ms.GetTableByName(tableName)
	if err != nil {
		return err
	}

	table.AcquireWrite()
	defer table.ReleaseWrite()

	_, err = sched.loadCSVData(table, tableName, csvPath, destCols, header)
	if err != nil {
		return fmt.Errorf("failed to load CSV data: %w", err)
	}

	fmt.Printf("CSV data loaded into table %s from %s\n", tableName, csvPath)

	return nil
}

func (sched *QueryScheduler) loadCSVData(
	table *metastore.Table,
	tableName string,
	csvPath string,
	destCols []string,
	hasHeader bool,
) (int, error) {
	file, err := os.Open(csvPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	var csvHeader []string
	if hasHeader {
		csvHeader, err = reader.Read()
		if err != nil {
			return 0, fmt.Errorf("failed to read CSV header: %w", err)
		}
	}

	colMapping := sched.buildColumnMapping(csvHeader, table, destCols, hasHeader)

	numCols := len(table.Columns)
	columnData := make([][]int64, numCols)
	strings := make([]strings.Builder, numCols)

	rowCount := 0

	// for {
	records, err := reader.ReadAll()
	if err != nil {
		return 0, fmt.Errorf("failed to read CSV row: %w", err)
	}

	for rowCount < len(records) {
		i := 0
		for ; i < deserializer.BatchSize && rowCount < len(records); i += 1 {
			record := records[rowCount]

			for csvIdx, strVal := range record {
				tableColIdx, ok := colMapping[csvIdx]
				if !ok {
					continue // Skip unmapped columns
				}
				colType := table.Columns[tableColIdx].Type

				// Parse value based on column type
				parsedVal, err := sched.parseValue(strVal, colType)
				if err != nil {
					return rowCount, fmt.Errorf("failed to parse value '%s' for column '%s': %w", strVal, table.Columns[tableColIdx].Name, err)
				}

				if colType == metastore.TypeInt {
					v := parsedVal.(int64)
					columnData[tableColIdx] = append(columnData[tableColIdx], v)
				} else if colType == metastore.TypeString {
					v := parsedVal.(string)
					columnData[tableColIdx] = append(columnData[tableColIdx], columnData[tableColIdx][len(columnData[tableColIdx])-1]+int64(len(v)))
					strings[tableColIdx].WriteString(v)
				}
			}
			rowCount += 1

			stringsMap := make(map[int]string)
			columnTypes := make([]byte, numCols)
			for _, val := range colMapping {
				colType := table.Columns[val].Type
				columnTypes[val] = byte(colType)

				if colType == metastore.TypeString {
					columnData[val] = append(columnData[val], int64(strings[val].Len()))
					stringsMap[val] = strings[val].String()
				}
			}

			batch := deserializer.Batch{
				BatchSize:   int32(i),
				NumColumns:  int32(numCols),
				ColumnTypes: columnTypes,
				Data:        columnData,
				String:      stringsMap,
			}

			serialize, _ := deserializer.NewSerializer(sched.ms.GetDataDir()+"/"+tableName, int32(rowCount), int32(numCols))
			err = serialize.WriteBatch(rowCount/deserializer.BatchSize, &batch)
			if err != nil {
				return rowCount, fmt.Errorf("failed to write batch file: %w", err)
			}

		}

	}

	return rowCount, nil
}

func (sched *QueryScheduler) parseValue(str string, colType metastore.ColumnType) (any, error) {

	switch colType {
	case metastore.TypeInt:
		val, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot parse '%s' as int: %w", str, err)
		}
		return val, nil
	case metastore.TypeString:
		return str, nil
	default:
		return nil, fmt.Errorf("unknown column type %d", colType)
	}
}

// CSV column index  -> table column index
func (sched *QueryScheduler) buildColumnMapping(
	csvHeader []string,
	table *metastore.Table,
	destCols []string,
	hasHeader bool,
) map[int]int {
	if len(destCols) == 0 || csvHeader == nil {

		// Direct mapping: CSV column i -> table column i
		mapping := make(map[int]int)
		for i := 0; i < len(table.Columns); i++ {
			mapping[i] = i
		}
		return mapping
	}

	mapping := make(map[int]int)

	for csvIdx, mapsTo := range destCols {
		for columnName, _ := range table.ColumnMapping {
			if columnName == mapsTo {
				mapping[csvIdx] = table.ColumnMapping[columnName]
				break
			}
		}
	}

	return mapping
}

// executeDelete handles DELETE query execution
func (sched *QueryScheduler) executeDelete(iq *internalQuery) error {

	tableName := iq.QueryDefinition.TableName

	err := sched.ms.DropTable(tableName)
	if err != nil {
		return fmt.Errorf("failed to drop table %s: %w", tableName, err)
	}

	log.Printf("Table %s deleted", tableName)

	return nil
}
