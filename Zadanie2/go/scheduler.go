package openapi

import (
	"Zadanie2/deserializer"
	"Zadanie2/metastore"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
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
	// log.Printf("Query scheduler started with %d workers", sched.numWorkers)
}

func (sched *QueryScheduler) Stop() {
	close(sched.stopChan)
	sched.wg.Wait()
	// log.Println("Query scheduler stopped")
}

func (sched *QueryScheduler) SubmitQuery(queryID string) {
	select {
	case sched.workQueue <- queryID:
		// log.Printf("Query %s submitted to scheduler", queryID)
	case <-sched.stopChan:
		// log.Printf("Scheduler stopped, cannot submit query %s", queryID)
	}
}

func (sched *QueryScheduler) worker(id int) {
	defer sched.wg.Done()
	// log.Printf("Worker %d started", id)

	for {
		select {
		case queryID := <-sched.workQueue:
			sched.executeQuery(id, queryID)
		case <-sched.stopChan:
			// log.Printf("Worker %d stopped", id)
			return
		}
	}
}

// executeQuery executes a single query based on its type
func (sched *QueryScheduler) executeQuery(workerID int, queryID string) {
	// log.Println("Worker starting executeQuery", queryID)
	iq, ok := sched.qs.get(queryID)
	// log.Println("executeQuery", iq.string())
	if !ok {
		// log.Printf("Worker %d: Query %s not found", workerID, queryID)
		return
	}

	defer func() {
		if iq.doneChan != nil {
			close(iq.doneChan)
		}
	}()
	// log.Printf("Worker %d: Executing query %s (SELECT=%v)", workerID, queryID, iq.IsSelect)

	// Update status to RUNNING
	now := time.Now()
	iq.SetRunning(now)

	var err error
	var resultRows QueryResultInner

	if iq.IsDelete {
		err = sched.executeDelete(iq)
	} else if iq.IsSelect {
		resultRows, err = sched.executeSelect(iq)
	} else {
		err = sched.executeLoad(iq)
	}

	// Update final status
	finished := time.Now()

	if err != nil {
		errMsg := err.Error()
		iq.SetFailed(finished, &MultipleProblemsError{
			Problems: []MultipleProblemsErrorProblemsInner{{Error: errMsg}},
		})
		// log.Printf("Worker %d: Query %s FAILED: %v", workerID, queryID, err)
	} else {
		iq.SetCompleted(finished, resultRows, iq.IsSelect)
		// log.Printf("Worker %d: Query %s COMPLETED", workerID, queryID)
	}
}

// executeSelect handles SELECT query execution
func (sched *QueryScheduler) executeSelect(iq *internalQuery) (QueryResultInner, error) {
	tableName := iq.QueryDefinition.TableName
	// log.Printf("Executing SELECT on table %s", tableName)
	table, err := sched.ms.GetTableByName(tableName)
	if err != nil {
		return QueryResultInner{}, err
	}

	table.AcquireRead()
	defer table.ReleaseRead()

	tableDataFiles := table.GetDataFiles()

	rows, err := sched.readTableData(table, tableDataFiles, iq.QueryDefinition)
	if err != nil {
		return QueryResultInner{}, err
	}

	return rows, nil
}

// readTableData reads data from table files and applies filtering/projection
func (sched *QueryScheduler) readTableData(table *metastore.Table, dataFiles []string, qd QueryQueryDefinition) (QueryResultInner, error) {

	allRows := QueryResultInner{}
	allRows.RowCount = int32(0)
	allRows.Columns = make([]QueryResultInnerColumnsInner, 0)

	tablePath := filepath.Join(sched.dataDir, table.Name)

	des, err := deserializer.NewBatchDeserializer(tablePath)
	if err != nil {
		return allRows, fmt.Errorf("failed to create deserializer: %w", err)
	}

	ints, stringsMap, err := des.ReadTableData()
	if err != nil {
		if err.Error() == "no column files found" {
			return allRows, nil
		}
		// log.Println("Error reading table data:", err)
		return allRows, fmt.Errorf("failed to read file: %w", err)
	}

	if len(ints) > 0 && len(ints[0]) > 0 {
		allRows.RowCount = int32(len(ints[0]))
	}
	allRows.Columns = make([]QueryResultInnerColumnsInner, len(ints))

	for idx, row := range ints {
		strCol, ok := stringsMap[idx]

		if ok {
			col := make([]interface{}, len(row)-1)
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

func (sched *QueryScheduler) executeLoad(iq *internalQuery) error {

	tableName := iq.QueryDefinition.DestinationTableName
	csvPath := iq.QueryDefinition.SourceFilepath

	destCols := iq.QueryDefinition.DestinationColumns
	header := iq.QueryDefinition.DoesCsvContainHeader

	// log.Printf("Executing LOAD into table %s from %s", tableName, csvPath)
	table, err := sched.ms.GetTableByName(tableName)
	if err != nil {
		return err
	}

	// log.Printf("Acquiring write lock for table %s", tableName)
	table.AcquireWrite()
	defer table.ReleaseWrite()

	// log.Printf("Loading CSV data into table %s from %s", tableName, csvPath)
	_, err = sched.loadCSVData(table, tableName, csvPath, destCols, header)
	if err != nil {
		return fmt.Errorf("failed to load CSV data: %w", err)
	}

	// fmt.Printf("CSV data loaded into table %s from %s\n", tableName, csvPath)

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

	// log.Printf("Building column mapping for table %s", tableName)
	colMapping, err := sched.buildColumnMapping(csvHeader, table, destCols, hasHeader)
	if err != nil {
		return 0, err
	}

	// log.Println("Column mapping:", colMapping)
	numCols := len(table.Columns)
	rowCount := 0

	// log.Printf("Starting to read CSV data for table %s", tableName)
	records, err := reader.ReadAll()
	if err != nil {
		return 0, fmt.Errorf("failed to read CSV row: %w", err)
	}

	if len(records) == 0 {
		return 0, nil
	}

	numCSVCols := len(records[0])
	if len(destCols) == 0 && numCSVCols != numCols {
		return 0, fmt.Errorf("CSV has %d columns but table has %d columns", numCSVCols, numCols)
	}

	if len(destCols) != 0 && len(destCols) != numCSVCols {
		return 0, fmt.Errorf("CSV has %d columns but destinationColumns has %d columns", numCSVCols, len(destCols))
	}

	for rowCount < len(records) {
		i := 0
		columnData := make([][]int64, numCols)
		strings := make([]strings.Builder, numCols)
		for ; i < deserializer.BatchSize && rowCount < len(records); i += 1 {
			record := records[rowCount]

			for csvIdx, strVal := range record {
				// log.Println("Record", record)
				tableColIdx, ok := colMapping[csvIdx]
				if !ok {
					continue
				}

				colType := table.Columns[tableColIdx].Type

				// log.Println("Parsing value", strVal, "as type", colType, " tableColIdx ", tableColIdx)
				parsedVal, err := sched.parseValue(strVal, colType)
				if err != nil {
					return rowCount, fmt.Errorf("failed to parse value '%s' for column '%s': %w", strVal, table.Columns[tableColIdx].Name, err)
				}

				if colType == metastore.TypeInt {
					v := parsedVal.(int64)
					columnData[tableColIdx] = append(columnData[tableColIdx], v)
				} else if colType == metastore.TypeString {
					v := parsedVal.(string)
					columnData[tableColIdx] = append(columnData[tableColIdx], int64(strings[tableColIdx].Len()))
					strings[tableColIdx].WriteString(v)
				}
			}
			rowCount += 1
		}
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

		tablePath := filepath.Join(sched.dataDir, table.Name)
		serialize, err := deserializer.NewSerializer(tablePath, int32(i), int32(numCols))
		if err != nil {
			return rowCount, fmt.Errorf("failed to create serializer: %w", err)
		}

		err = serialize.WriteBatch(rowCount/deserializer.BatchSize, &batch)
		if err != nil {
			return 0, fmt.Errorf("failed to write batch file: %w", err)
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
) (map[int]int, error) {
	if len(destCols) == 0 {
		mapping := make(map[int]int)
		for i := 0; i < len(table.Columns); i++ {
			mapping[i] = i
		}
		return mapping, nil
	}

	mapping := make(map[int]int)

	hit := 0
	for csvIdx, mapsTo := range destCols {
		_, ok := table.ColumnMapping[mapsTo]
		if ok {
			mapping[csvIdx] = table.ColumnMapping[mapsTo]
			hit += 1
			continue
		}
	}

	if hit != len(table.Columns) {
		return mapping, fmt.Errorf("the provided destinationColumns do not cover all table columns")
	}

	// log.Println("mapping built:", mapping)

	return mapping, nil
}

func (sched *QueryScheduler) executeDelete(iq *internalQuery) error {

	tableID := iq.QueryDefinition.TableName
	table, err := sched.ms.GetTableById(tableID)
	if err != nil {
		return err
	}

	err = sched.ms.DropTable(table.Name)
	if err != nil {
		return fmt.Errorf("failed to drop table %s: %w", table.Name, err)
	}

	// log.Printf("Table %s deleted", table.Name)

	return nil
}
