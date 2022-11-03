-- This file duplicates the commands in gtfs::load_gtfs
-- it is intended to permit easier copy-pasting for performance work

.mode csv
.timer on
.eqp on

.import '/Users/alexjago/Documents/Projects/fluvial/data/201602_gtfs/calendar.txt' Calendar
.import '/Users/alexjago/Documents/Projects/fluvial/data/201602_gtfs/routes.txt' Routes_VIRT
.import '/Users/alexjago/Documents/Projects/fluvial/data/201602_gtfs/stops.txt' Stops_VIRT
.import '/Users/alexjago/Documents/Projects/fluvial/data/201602_gtfs/stop_times.txt' StopTimes_VIRT
.import '/Users/alexjago/Documents/Projects/fluvial/data/201602_gtfs/trips.txt' Trips_VIRT



-- it's about 5-10 seconds at the console to do this import but much faster in fluvial for some reason?

--CREATE VIRTUAL TABLE Routes_VIRT USING csv(filename='/Users/alexjago/Documents/Projects/fluvial/data/201602_gtfs/routes.txt', header=YES);
--CREATE VIRTUAL TABLE Stops_VIRT USING csv(filename='/Users/alexjago/Documents/Projects/fluvial/data/201602_gtfs/stops.txt', header=YES);
--CREATE VIRTUAL TABLE StopTimes_VIRT USING csv(filename='/Users/alexjago/Documents/Projects/fluvial/data/201602_gtfs/stop_times.txt', header=YES);
--CREATE VIRTUAL TABLE Trips_VIRT USING csv(filename='/Users/alexjago/Documents/Projects/fluvial/data/201602_gtfs/trips.txt', header=YES);


-- be more minimalistic about Routes
CREATE TABLE Routes (route_id TEXT PRIMARY KEY, route_short_name TEXT);
INSERT INTO Routes (route_id, route_short_name)
    SELECT route_id, route_short_name FROM Routes_VIRT;

CREATE TABLE Stops (stop_id INT PRIMARY KEY, stop_name TEXT, stop_lat REAL, stop_lon REAL);
INSERT INTO Stops (stop_id, stop_name, stop_lat, stop_lon) 
    SELECT stop_id, stop_name, stop_lat, stop_lon FROM Stops_VIRT;

CREATE TABLE Trips (route_id TEXT, service_id TEXT, trip_id TEXT PRIMARY KEY, direction_id TEXT, shape_id TEXT, 
    FOREIGN KEY(route_id) REFERENCES Routes(route_id));
INSERT INTO Trips (route_id, service_id, trip_id, direction_id, shape_id)
    SELECT route_id, service_id, trip_id, direction_id, shape_id FROM Trips_VIRT;

CREATE TABLE StopTimes (trip_id TEXT, stop_id INT, stop_sequence INT, 
    FOREIGN KEY (trip_id) REFERENCES Trips(trip_id), 
    FOREIGN KEY (stop_id) REFERENCES Stops(stop_id) 
    );
INSERT INTO StopTimes (trip_id, stop_id, stop_sequence)
   SELECT trip_id, stop_id, stop_sequence FROM StopTimes_VIRT;

-- about 0.75 sec to here in the console    

-----------------------------------------------------------------------------------------------------------------
-- this is the slow part

-- What we're trying to get here is a somewhat denormalised table of 
-- stop_id, stop_sequence, direction_id, route_short_name, shape_id, {count of these}
-- since the same route IDs can have different service patterns we really have to use shape_id,
-- but we want to be able to look things up by (route, direction)

-- Let's take this from the inside out. 

-- StopTimes (well, our distillation) has a trip_id, a stop_id and a sequence_id
-- We cross-ref that with Trips to turn our trip_id into a direction, route and shape
CREATE VIEW TripSeqs AS
        SELECT S.stop_id, S.stop_sequence, Trips.direction_id, Trips.route_id, Trips.shape_id
        FROM StopTimes S
        INNER JOIN Trips on S.trip_id = Trips.trip_id;
        
-- Then we group and count...
CREATE VIEW TripSeqCounts(stop_id, stop_sequence, direction_id, route_id, shape_id, qty)
        AS SELECT stop_id, stop_sequence, direction_id, route_id, shape_id, Count(*)
        FROM TripSeqs
        GROUP BY stop_id, stop_sequence, direction_id, route_id, shape_id;
        
-- and exchange route_id for route_short_name
-- interestingly there's about 1000 rows that conglomerated going route_id -> route_short_name
CREATE VIEW SSI (stop_id, stop_sequence, direction_id, route_short_name, shape_id, qty)
        AS SELECT stop_id, stop_sequence, direction_id, route_short_name, shape_id, SUM(qty)
        FROM TripSeqCounts
        INNER JOIN Routes ON TripSeqCounts.route_id = Routes.route_id
        GROUP BY stop_id, stop_sequence, direction_id, route_short_name, shape_id;
        
-- and materialize
CREATE TABLE StopSeqs AS SELECT * FROM SSI;
-- about 4.2 seconds for this in particular

-- We will later search by both these terms so a dual-column index is very useful
CREATE INDEX ss_routedir ON StopSeqs(route_short_name, direction_id);

-- We will also search by shape_id
CREATE INDEX ss_shapeid ON StopSeqs(shape_id);
-- and another 0.1 sec for this

-----------------------------------------------------------------------------------------------------------------

-- We can't do the group-and-count until we've thrown away the trip_id, huh
-- We might be able to save half a second by not copying StopTimes but just pulling from _VIRT?
--      tried it, that probably just transferred thing arund
-- if we index on routes(route_id) does that improve things?
-- never mind that, we escalated all the way to doing PKs and FKs properly. Seems faster. 

-- experimental...
/*
CREATE TABLE SS2 (stop_id INT, stop_sequence INT, direction_id TEXT, route_short_name TEXT, shape_id TEXT, qty INT,
    FOREIGN KEY(stop_id) REFERENCES Stops(stop_id), 
    FOREIGN KEY(route_short_name) REFERENCES Routes(route_short_name), 
    FOREIGN KEY(shape_id) REFERENCES Trips(shape_id));
INSERT INTO SS2 SELECT S.stop_id, S.stop_sequence, T.direction_id, R.route_short_name, T.shape_id, count(*)
    from StopTimes S
    inner join Trips T on S.trip_id = T.trip_id
    inner join Routes R on T.route_id = R.route_id
    group by stop_id, stop_sequence, direction_id, route_short_name, shape_id;
*/
-- huh, this was slower
-- I guess having all-in data at the group by is worse than having two with less data to chug through

-- potentially, we can do the *count* according to grouping (route, direction, shape_id) up with min(trip_id)
-- (if it's the same route/dir/shape then I think that's a sufficiently low chance of being a different sequence)

-- CREATE INDEX idx_stoptimes_trips ON StopTimes(trip_id, stop_id, stop_sequence);

-- Experiment: covering index on the virtual table
-- Result: fails miserably
CREATE INDEX idx_stoptimes ON StopTimes_VIRT(trip_id, stop_id, stop_sequence);

CREATE VIEW RSC AS SELECT route_short_name, direction_id, shape_id, count(trip_id) as qty, min(trip_id) as trip_id 
    FROM Trips INNER JOIN Routes ON Trips.route_id = Routes.route_id 
    GROUP BY route_short_name, direction_id, shape_id;

CREATE TABLE RouteShapeCounts AS SELECT * FROM RSC;

CREATE VIEW backdoor (stop_id, stop_sequence, direction_id, route_short_name, shape_id, qty) AS 
    select S.stop_id, S.stop_sequence, R.direction_id, R.route_short_name, R.shape_id, R.qty 
    FROM RouteShapeCounts R, StopTimes_VIRT S WHERE R.trip_id = S.trip_id;

CREATE TABLE StopSeqsTwo AS SELECT * FROM backdoor;

-----------------------------------------------------------------------------------------------------------------


-- Service intensity code

-- n.b. this was failing at one point because I had stupidly removed service_id from Trips

CREATE VIEW RTI (trip_id, service_id, route_short_name, direction_id)
    AS SELECT trip_id, service_id, route_short_name, direction_id
    FROM Trips INNER JOIN Routes ON Routes.route_id = Trips.route_id;

CREATE VIEW RTF (service_id, route_short_name, direction_id, freq)
    AS select service_id, route_short_name, direction_id, count(*)
    FROM RTI GROUP BY service_id, route_short_name, direction_id;

CREATE VIEW SDI (route_short_name, direction_id, freq, monday, tuesday, wednesday, thursday, friday, saturday, sunday)
    AS SELECT route_short_name, direction_id, freq, monday, tuesday, wednesday, thursday, friday, saturday, sunday
    FROM RTF, Calendar WHERE Calendar.service_id = RTF.service_id;

CREATE TABLE ServiceCounts (route_short_name TEXT, direction_id TEXT, freq INTEGER, 
        monday INTEGER, tuesday INTEGER, wednesday INTEGER, 
        thursday INTEGER, friday INTEGER, saturday INTEGER, sunday INTEGER);

INSERT INTO ServiceCounts SELECT * FROM SDI;
-- and about 0.15 sec from RTI to here

-- total about 15 seconds? huh, the actual entire runtime was lower