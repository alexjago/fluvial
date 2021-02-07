#!/bin/bash

# Generate a CSV of routes with their default number of services per day (each direction)

if [[ $# -ne 1 ]]; then
  >&2 echo "USAGE: routecounter.sh GTFS_DIR  [ > output.csv ]"
else
cd "$1"

# motivation: we want stops used by routes that are minimally frequent: 4vph * 12h

# this wasn't working very well in SQLite so let's use xsv
xsv search -s 5 '^1$' < stop_times.txt | xsv select 1,2 > stop_firsts.txt

for day in monday tuesday wednesday thursday friday saturday sunday
do

echo ".mode csv
.import calendar.txt Calendar
.import stop_firsts.txt StopFirsts
.import trips.txt Trips
.import routes.txt Routes
.headers on

CREATE VIEW DayServices AS SELECT service_id FROM Calendar WHERE $day = '1';
CREATE VIEW DayTrips AS SELECT trip_id from Trips WHERE Trips.service_id IN DayServices;
CREATE VIEW DayStops AS SELECT * from StopFirsts WHERE trip_id IN DayTrips;

CREATE VIEW TripFirsts AS SELECT DISTINCT route_id, arrival_time, service_id, direction_id
FROM DayStops, Trips
WHERE DayStops.trip_id = Trips.trip_id;

CREATE VIEW RouteStarts AS SELECT route_short_name, direction_id, arrival_time, service_id
FROM TripFirsts, Routes WHERE TripFirsts.route_id = Routes.route_id;

CREATE VIEW RouteCounts AS select count(*) as cnt, route_short_name, service_id, direction_id
FROM RouteStarts GROUP BY route_short_name, service_id, direction_id; 

CREATE VIEW RoutesDisam AS select max(cnt) as qty, route_short_name, direction_id 
FROM RouteCounts GROUP BY route_short_name, direction_id;

SELECT route_short_name as route, direction_id, qty as $day FROM RoutesDisam
ORDER BY route_short_name, direction_id;
" | sqlite3 > $day.csv
done

# SQLite doesn't have full outer joins so this next bit is a little messy
# also there's no good way to loop this, so...

echo ".mode csv
.headers on
.import monday.csv Monday
.import tuesday.csv Tuesday
.import wednesday.csv Wednesday
.import thursday.csv Thursday
.import friday.csv Friday
.import saturday.csv Saturday
.import sunday.csv Sunday

CREATE VIEW Alpha AS 
SELECT Monday.route, Monday.direction_id, monday, tuesday 
FROM Monday LEFT JOIN Tuesday USING(route, direction_id)
UNION ALL
SELECT Tuesday.route, Tuesday.direction_id, monday, tuesday 
FROM Tuesday LEFT JOIN Monday USING(route, direction_id) WHERE Monday.route IS NULL;

CREATE VIEW Beta AS 
SELECT Alpha.route, Alpha.direction_id, monday, tuesday, wednesday 
FROM Alpha LEFT JOIN Wednesday USING(route, direction_id)
UNION ALL
SELECT Wednesday.route, Wednesday.direction_id, monday, tuesday, wednesday 
FROM Wednesday LEFT JOIN Alpha USING(route, direction_id) WHERE Alpha.route IS NULL;

CREATE VIEW Gamma AS 
SELECT Beta.route, Beta.direction_id, monday, tuesday, wednesday, thursday
FROM Beta LEFT JOIN Thursday USING(route, direction_id)
UNION ALL
SELECT Thursday.route, Thursday.direction_id, monday, tuesday, wednesday, thursday
FROM Thursday LEFT JOIN Beta USING(route, direction_id) WHERE Beta.route IS NULL;

CREATE VIEW Delta AS 
SELECT Gamma.route, Gamma.direction_id, monday, tuesday, wednesday, thursday, friday
FROM Gamma LEFT JOIN Friday USING(route, direction_id)
UNION ALL
SELECT Friday.route, Friday.direction_id, monday, tuesday, wednesday, thursday, friday
FROM Friday LEFT JOIN Gamma USING(route, direction_id) WHERE Gamma.route IS NULL;

CREATE VIEW Epsilon AS 
SELECT Delta.route, Delta.direction_id, monday, tuesday, wednesday, thursday, friday, saturday
FROM Delta LEFT JOIN Saturday USING(route, direction_id)
UNION ALL
SELECT Saturday.route, Saturday.direction_id, monday, tuesday, wednesday, thursday, friday, saturday
FROM Saturday LEFT JOIN Delta USING(route, direction_id) WHERE Delta.route IS NULL;

SELECT Epsilon.route, Epsilon.direction_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday
FROM Epsilon LEFT JOIN Sunday USING(route, direction_id)
UNION ALL
SELECT Sunday.route, Sunday.direction_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday
FROM Sunday LEFT JOIN Epsilon USING(route, direction_id) WHERE Epsilon.route IS NULL;
" | sqlite3 

# Clean up!

for day in monday tuesday wednesday thursday friday saturday sunday
do
rm $day.csv
done

rm stop_firsts.txt

fi
