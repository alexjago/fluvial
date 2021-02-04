#! /bin/bash
# Create a positional crosstab (next step: pivot table)

if [[ $# -ne 2 ]]; then
    >&2 echo "usage: crosstabber.sh patronage.csv positions.csv > output.csv"
else

echo ".mode csv
.import $1 Patronage
.import $2 Positions
.headers on

CREATE VIEW ITab AS SELECT origin_stop, destination_stop, sum(quantity) as qty
FROM Patronage WHERE operator = 'Queensland Rail' GROUP BY origin_stop, destination_stop;

CREATE VIEW JTab AS SELECT DISTINCT destination_stop, stop_name as origin, qty
FROM ITab 
LEFT JOIN Positions O ON O.stop_id = origin_stop;

SELECT DISTINCT origin, stop_name as destination, qty as quantity
FROM JTab 
LEFT JOIN Positions D ON D.stop_id = destination_stop;
" | sqlite3

fi