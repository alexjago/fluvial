-- Helper script for generating a full positions file from GTFS data
-- First create a positions_config.csv file and save it alongside the GTFS data

-- Then run the following from the shell:
-- cd path/to/gtfs
-- sqlite3 < path/to/rail_position_helpers.sql > positions.csv

-- Once that is done, edit positions.csv to fix up any weightings on combined sections,
-- deduplicate stations across other lines, rename stations from other lines as those lines
-- and generally file to fit on assembly

.mode csv

-- Config file
.import "positions_config.csv" Configuration
-- Usual GTFS files
.import "routes.txt" Routes
.import "trips.txt" Trips
.import "stops.txt" Stops
.import "stop_times.txt" StopTimes

-- helper index
CREATE INDEX STIdx ON StopTimes (trip_id, stop_id);

-- A helper table of all the basic routes
CREATE TABLE LineStations AS
WITH 
    SelectedRoutes AS (
        SELECT DISTINCT
            route_short_name, route_id
        FROM Routes
        WHERE Routes.route_short_name IN
            (SELECT DISTINCT route_short_name FROM Configuration)
    ),
    LineStopParents AS (
    SELECT
        SelectedRoutes.route_short_name,
        Stops.parent_station,
        max(cast(StopTimes.stop_sequence as int)) as est_seq
    FROM
        Stops,
        SelectedRoutes,
        Trips,
        StopTimes,
        Configuration
    WHERE
        SelectedRoutes.route_id = Trips.route_id
        and Trips.trip_id = StopTimes.trip_id
        and StopTimes.stop_id = Stops.stop_id
        and Stops.stop_id >= 600000
    GROUP BY
        SelectedRoutes.route_short_name,
        Stops.parent_station
    ORDER BY
        SelectedRoutes.route_short_name,
        est_seq
    ),
    ParentNames AS (
        SELECT DISTINCT
            Stops.stop_name,
            LineStopParents.parent_station
        FROM LineStopParents, Stops
        WHERE
            LineStopParents.parent_station = Stops.stop_id
    )
SELECT DISTINCT
    LineStopParents.route_short_name,
    ParentNames.stop_name,
    Stops.stop_id,
    LineStopParents.est_seq
FROM
    Stops, LineStopParents, ParentNames
WHERE
    LineStopParents.parent_station = ParentNames.parent_station
    and LineStopParents.parent_station = Stops.parent_station
ORDER BY 
    LineStopParents.route_short_name, est_seq
;

.headers on

-- The main event
-- First set up a helper view (SelectedLines) to list stop_ids in the target lines
-- Then also set further restrictions 
-- >= 600000 is just because SEQ train station stops are all that
-- then also (a) either take the target line
-- or (b) take the *first* other line

WITH
SelectedLines (route_name, direction, route_short_name, stop_id) AS 
(SELECT distinct Configuration.route_name, Configuration.direction, Configuration.route_short_name, LineStations.stop_id 
    FROM Configuration, LineStations 
    WHERE LineStations.route_short_name IS Configuration.route_short_name and Configuration.weighting = 1
    ORDER BY Configuration.route_name, Configuration.direction)--,
-- Fudge AS 
-- (select distinct S.route_name, S.direction, S.route_short_name, est_seq, count(stop_sequence) as stop_seqs
--     from SelectedLines S 
--         inner join LineStations L 
--         on S.route_short_name = L.route_short_name  
--         inner join Configuration C 
--         on S.route_name = C.route_name and S.direction = C.direction and S.route_short_name = C.route_short_name
--     HAVING est_seq > cast(stop_sequence as int)
-- )
SELECT DISTINCT C.route_name, C.direction, L.stop_name, C.lookup_route, C.lookup_direction, C.stop_sequence, C.weighting, L.stop_id, L.route_short_name, 
    CASE C.weighting
        WHEN 1 THEN C.stop_sequence + L.est_seq - 1
        ELSE C.stop_sequence 
    END as est_seq
FROM Configuration C, LineStations L
WHERE C.route_short_name IS L.route_short_name 
    AND CAST(L.stop_id AS INT) >= 600000
    AND (C.weighting = 1 OR (L.stop_id NOT IN 
        (SELECT stop_id FROM SelectedLines S
            WHERE S.route_name IS C.route_name 
            AND S.direction IS C.direction) AND 
            NOT EXISTS (SELECT G.stop_sequence FROM Configuration G, LineStations T
                WHERE G.route_name IS C.route_name AND G.direction IS C.direction 
                    AND G.route_short_name = T.route_short_name AND T.stop_id = L.stop_id
                    AND G.stop_sequence < C.stop_sequence
            ))
        )
ORDER BY C.route_name, C.direction, C.stop_sequence, L.est_seq
;

-- remaining todo for inserts maths
-- count the number of inserts that are greater than the selected line's base 
-- and are <= est_seq; add that
