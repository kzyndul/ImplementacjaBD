# Implementacja systemów baz danych
<b> Rozwiązanie drugiego zadania z ISBD MIMUW 2025 </b> <br />
<b> Autor: Krzysztof Żyndul </b>

## Kompilacja

Każda kolumna w oddzielnym pliku, mam

heder stałej długości np typ, pliku, długość batcha, gdzie zaczyna się footer

footer: dla każdego batcha informacje potrzebne do dekompresji i miejsce w którym sie zaczyna 


dla napisów trzymam diwe wartosci tablice offsetów gdzie zaczyna sie konkretny napis + ostatni napis długość całkowita danych i druga wartosć to jest concatenacja 
wszystkich napisów w batchu do jednego poprzedzielana czyms zeby wiadomo gdzie jest koniec.





Nagłówek pliku, typ, 
    