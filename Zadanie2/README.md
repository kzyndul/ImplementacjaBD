# Implementacja systemów baz danych
<b> Rozwiązanie drugiego zadania z ISBD MIMUW 2025 </b> <br />
<b> Autor: Krzysztof Żyndul </b>

## Kompilacja

run 


Format pliku:
- Nagłówek pliku: 
    - BatchSize - ilość wierszy w tym pliku
    - NumColumns - ikość kolumn w tym pliku
    - FooterOffset

- Dane

- Stopka pliku: dla każdego kolumny informacja:
    - Jakiego typu jest ta kolumna
    - Jaka jest warotść najmniejsze liczby w tablicy intów
    - Miejsce w ktorym zaczynaj sie skompresowana tablica intów
    - Miejsce w ktorym zaczynaja sie skompresowana tablca stringów lub 0 jezeli typ to int

## Kompresja
Do kompresji Intów używany jest delta enkoding połaczony z VLT. Dla napisów: cała tablica jest konkatenowana w jeden długi napis oddzielany "\0" i dla każdego elemenetu zapamiętywany jest ofset na kórej pozycji zaczyna się on w tym skonkatenowanym napisie.