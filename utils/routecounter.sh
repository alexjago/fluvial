# Generate 

if [[ $# -ne 1 ]]; then
&>2 echo "USAGE: ./frequentstopper.sh GTFS_DIR > output.csv"
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
CREATE VIEW TripFirsts AS SELECT DISTINCT route_id, arrival_time, service_id 
FROM DayStops, Trips
WHERE DayStops.trip_id = Trips.trip_id;
CREATE VIEW RouteStarts AS SELECT route_short_name, arrival_time, service_id
FROM TripFirsts, Routes WHERE TripFirsts.route_id = Routes.route_id;
CREATE VIEW RouteCounts AS select count(*) as cnt, route_short_name, service_id from RouteStarts GROUP BY route_short_name, service_id; 
CREATE VIEW RoutesDisam AS select max(cnt) as qty, route_short_name 
FROM RouteCounts GROUP BY route_short_name;
SELECT route_short_name as route, qty as $day FROM RoutesDisam
ORDER BY qty DESC;
" | sqlite3 > $day.csv
done

paste -d "," monday.csv tuesday.csv wednesday.csv thursday.csv friday.csv saturday.csv sunday.csv | cut -d "," -f 1,2,4,6,8,10,12,14 

# cleanup
for day in monday tuesday wednesday thursday friday saturday sunday
do
rm $day.csv
done
rm stop_firsts.txt

fi
