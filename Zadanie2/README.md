# Implementacja systemów baz danych
<b> Rozwiązanie drugiego zadania z ISBD MIMUW 2025 </b> <br />
<b> Autor: Krzysztof Żyndul </b>

## Kompilacja i uruchomienie
go build
./Zadanie2 <table_folder>



## Kompresja danych
1. Kompresja wartości całkowitych (int64)
  - Delta encoding
  - Variable-length encoding

2. Kompresja tablic napisów
Tablice napisów są kompresowane w kilku krokach:
  - Wszystkie elementy tablicy są konkatenowane w jeden długi ciąg znaków.
  - Tworzona jest tablica offsetów, określająca, gdzie zaczyna się i kończy każdy oryginalny napis.
  - Skonkatenowany ciąg znaków jest kompresowany algorytmem LZ4.


## Format pliku
Każdy plik danych składa się z czterech części:

1. Nagłówek pliku - podstawowe metadane: 
    - BatchSize – liczba wierszy,
    - NumColumns - liczba kolumn,
    - FooterOffset - offset, pozycja w bajtach, w którym zaczyna się stopka pliku.


2. Dane - dane są przechowywane w postaci tablicy dwuwymiarowej, w której:
 - każdy wiersz odpowiada kolumnie w tabeli,
 - wszystkie dane numeryczne (int64) są skompresowane przy użyciu variable-length encoding oraz delta encoding.
 - dla kolumn tekstowych tablica zawiera BatchSize + 1 elementów (offsety początków i końców napisów).

3. Stopka pliku - metadane opisujące kolumny, ich typy, lokalizację oraz informacje o kompresji:
 - DeltaDelta       int64	wartość do dekodowania tablicy ColumnsDelta,
 - DeltaOffset	    int64	wartość do dekodowania tablicy ColumnsOffset,
 - Offset1	        int64	offset końca tablicy ColumnsType,
 - Offset2	        int64	offset końca tablicy ColumnsDelta,
 - StringOffset	    int64	offset końca tablicy ColumnsOffset = początku skompresowanych napisów,
 - StringSize	    int64	rozmiar skompresowanego ciągu znaków,
 - ColumnsType	    []byte	typy każdej z kolumn, 
 - ColumnsDelta	    []int64	wartość do dekodowania każdej z kolumn kolumny,
 - ColumnsOffset	[]int64	offset gdzie dana kolumna się zaczyna (BatchSize + 1).

4. Skompresowany string, który jest konkatenacją wszystkich napisów ze wszystkich kolumn. Ten ciąg znaków jest kompresowany przy użyciu LZ4, aby zminimalizować rozmiar pliku.

## Batch
Struktura batcha:
 - BatchSize    int32       ilość wierszy w batchu,
 - NumColumns   int32       ilość kolumn w batchu,
 - ColumnTypes  []byte      tyo każdej z kolumny,
 - Data         [][]int64   dane dla każdej z kolumn,
 - String       string      skonkatenowany wszystkie napsiy z kazdej z kolumn.
