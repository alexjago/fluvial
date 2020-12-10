-- This is an SQLite3 commands file to help generate a
-- railway positions file for Fluvial, from GTFS data

-- run it from the shell:
-- sqlite3 < genpositions.sql > positions.csv

-- you should either run it with your current working directory set to
-- the GTFS directory, or change the paths below on lines 9-15.

.mode csv

.import "routes.txt" Routes
.import "trips.txt" Trips
.import "stops.txt" Stops
.import "stop_times.txt" StopTimes

CREATE VIEW NamedPlacesI AS SELECT DISTINCT stop_id, parent_station FROM Stops WHERE parent_station IN (SELECT stop_id FROM STOPS WHERE location_type = 1);
CREATE TABLE NamedPlaces AS SELECT NamedPlacesI.stop_id, Stops.stop_name from NamedPlacesI, Stops WHERE NamedPlacesI.parent_station = Stops.stop_id;

INSERT INTO NamedPlaces SELECT stop_id, stop_name FROM Stops WHERE location_type = 1;

-- select * from NamedPlaces ORDER BY stop_name;

CREATE VIEW RailRoutes AS SELECT DISTINCT route_id FROM Routes WHERE route_type = 2;
CREATE VIEW RailTrips AS SELECT DISTINCT trip_id, RailRoutes.route_id FROM Trips INNER JOIN RailRoutes on Trips.route_id = RailRoutes.route_id;

CREATE TABLE StopTimesT AS SELECT DISTINCT trip_id, stop_id, stop_sequence FROM StopTimes;

CREATE TABLE RailStopsT AS SELECT DISTINCT stop_id, stop_sequence, route_id FROM StopTimesT INNER JOIN RailTrips ON StopTimesT.trip_id = RailTrips.trip_id;

CREATE TABLE RailTripStops AS SELECT DISTINCT N.stop_id, R.stop_sequence, R.route_id, N.stop_name
 FROM RailStopsT R, NamedPlaces N WHERE N.stop_name IN
 (SELECT stop_name from NamedPlaces P where P.stop_id = R.stop_id);

CREATE TABLE RailStops AS SELECT DISTINCT R.route_short_name, T.stop_id, T.stop_sequence, T.stop_name
FROM RailTripStops T INNER JOIN Routes R ON R.route_id = T.route_id;

.headers on

select * from RailStops;
