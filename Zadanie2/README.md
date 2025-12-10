# Implementacja systemów baz danych
**Rozwiązanie drugiego zadania z ISBD MIMUW 2025**  
**Autor: Krzysztof Żyndul**

## Kompilacja i uruchomienie

### Lokalne uruchomienie
```bash
make build
./dbms
```

### Docker
```bash
make docker
docker run -d -p 8080:8080 -v $(pwd)/data:/data dbms:latest
```

Aplikacja będzie dostępna pod adresem `http://localhost:8080`, a interfejs Swagger UI pod `http://localhost:8080/docs`.

## Architektura systemu

### Struktura katalogów
```
.
├── data/               # Dane tabel (katalog dla każdej tabeli)
│   └── <table_name>/
│       └── column_*.dat
├── metastore.json      # Metadane tabel (schematy, typy kolumn)
├── main.go             # Główny plik aplikacji
├── metastore/          # Moduł zarządzający metadanymi i dostępem do tabel
├── go/                 # Pliki wygenerowane przez OpenAPI Generator
└── deserializer/       # Moduł implementujący serializację i deserializację
```

### Komponenty systemu

#### 1. Metastore (`metastore/`)
Zarządza metadanymi tabel:
- Schematy tabel (nazwy, kolumny, typy)
- Mapowanie nazw kolumn na indeksy
- Ścieżki do plików danych
- Timestamps (utworzenie, ostatnia modyfikacja)
- Blokady read/write na tabelach (RWMutex)

Dane przechowywane w `metastore.json`, ładowane przy starcie i zapisywane przy zamknięciu.

#### 2. API
API zostało wygenerowane przy użyciu **OpenAPI Generator** na podstawie specyfikacji w pliku `dbmsInterface.yaml`. Główne pliki to:
- `api_proj3_service.go` - implementacja endpointów API
- `scheduler.go` - harmonogramowanie i wykonywanie zapytań
- `query_store.go` - przechowywanie stanu zapytań

#### 3. Query Scheduler (`scheduler.go`)
- Obsługuje zapytania asynchronicznie, wysyłając je do workerów
- Implementuje wykonanie zapytań

#### 4. Query Store (`query_store.go`)
- Przechowuje listę wszystkich zapytań
- Wyniki zapytań przechowywane są w pamięci

#### 5. Serializer/Deserializer (`deserializer/`)
Odpowiada za zapis i odczyt danych:
- **Serializer**: zapisuje dane w batchach do plików `column_*.dat`
- **Deserializer**: odczytuje i dekompresuje dane z plików

## Format danych

### Batch
Dane przetwarzane w batchach po **8192 wiersze**:
```go
type Batch struct {
    BatchSize   int32           // Liczba wierszy
    NumColumns  int32           // Liczba kolumn
    ColumnTypes []byte          // Typ każdej kolumny (0=int, 1=string)
    Data        [][]int64       // Dane kolumnowe
    String      map[int]string  // Skonkatenowane stringi dla kolumn tekstowych
}
```

### Struktura pliku `column_*.dat`
Każda kolumna jest przechowywana w oddzielnym pliku. Każdy plik składa się z:

1. **Header (13 bajtów)**
   - `ColumnType` (1 byte): 0=int, 1=string
   - `NumBatches` (4 bytes): liczba batchy w pliku
   - `FooterOffset` (8 bytes): offset do footera

2. **Batche**
   - Skompresowane dane int64 (delta + variable-length encoding)
   - Dla stringów dodatkowo skompresowane (LZ4) stringi

3. **Footer**
   - `BatchOffsets[]`: gdzie zaczyna się każdy batch
   - `BatchDeltas[]`: wartości delta dla dekompresji
   - `StringSizes[]`: rozmiary skompresowanych stringów

# Znane ograniczenia

**RWMutex w Go nie gwarantuje sprawiedliwości** - pisarze mogą głodzić czytelników, a wątki nie są kolejkowane w kolejce FIFO. **Kolejność wykonania zapytań może różnić się od kolejności ich przyjęcia do systemu.**

## destinationColumns

Parametr ten zinterpretowałme w następujący sposób. Niech plik źródłowy CSV am n kolumn, wted destinationColumns powinna mieć n wartości. Jeżeli i-tą wartością parametru destinationColumns jest nazwa j-otej kolumny tabeli docelowej oznacza to, że i-ta kolumna powinna pliku CSV być zmapowana na j-tą kolumne tabeli.     