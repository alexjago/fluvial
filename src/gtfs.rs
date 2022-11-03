//! Functions for dealing with GTFS...

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::iter::Iterator;
use std::path::PathBuf;

use rusqlite::{Connection, Result};

use serde::{Deserialize, Serialize};
use serde_rusqlite::from_rows;

use super::{convert_direction, days_per_month, get_boardings, Path};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct StopSeq {
    stop_id: i64,
    stop_sequence: i64,
    shape_id: String,
    qty: i64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct ServiceCounts {
    freq: i32,
    monday: i8,
    tuesday: i8,
    wednesday: i8,
    thursday: i8,
    friday: i8,
    saturday: i8,
    sunday: i8,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct FirstLastSeq {
    first: i64,
    last: i64,
    shape_id: String,
    len: i64,
    qty: i64,
}

pub fn load_gtfs(db: &Connection, gtfs_dir: &Path) -> Result<(), rusqlite::Error> {
    //! Loads all the GTFS CSVs into `SQLite` tables in `db`
    // let now = std::time::Instant::now();

    let mut dir: PathBuf = PathBuf::from(gtfs_dir);

    for (t, p) in &[
        ("Calendar", "calendar.txt"),
        ("Routes", "routes.txt"),
        ("Stops", "stops.txt"),
        ("StopTimes", "stop_times.txt"),
        ("Trips", "trips.txt"),
    ] {
        dir.push(&p);

        let schema = format!(
            "CREATE VIRTUAL TABLE {}_VIRT USING csv(filename='{}', header=YES)",
            &t,
            &dir.as_path().display()
        );
        dir.pop();

        //eprintln!("{}", schema);

        db.execute_batch(&schema)?;

        //         let schema = format!("CREATE TABLE {} AS SELECT * FROM {}_VIRT", &t, &t);
        //         db.execute_batch(&schema)?;
    }

    // eprintln!(
    //     "Info: GTFS virtual tables at {} ms.",
    //     now.elapsed().as_millis()
    // );

    // pre-creating tables is what makes this perform at a reasonable speed
    db.execute_batch("CREATE TABLE Calendar AS SELECT * FROM Calendar_VIRT;")?;

    db.execute_batch("CREATE TABLE Routes (route_id TEXT PRIMARY KEY, route_short_name TEXT);")?;
    db.execute_batch(
        "INSERT INTO Routes (route_id, route_short_name)
    SELECT route_id, route_short_name FROM Routes_VIRT;",
    )?;

    db.execute_batch(
        "CREATE TABLE Stops (stop_id INT PRIMARY KEY, stop_name TEXT, stop_lat REAL, stop_lon REAL);",
    )?;
    db.execute_batch(
        "INSERT INTO Stops (stop_id, stop_name, stop_lat, stop_lon) SELECT stop_id, stop_name, stop_lat, stop_lon FROM Stops_VIRT;")?;
    // Pretty much all of Trips is stringly-typed but we still want a PK
    db.execute_batch("CREATE TABLE Trips (route_id TEXT,  service_id TEXT, trip_id TEXT PRIMARY KEY, direction_id TEXT, shape_id TEXT, 
        FOREIGN KEY(route_id) REFERENCES Routes(route_id));")?;
    db.execute_batch(
        "INSERT INTO Trips (route_id, service_id, trip_id, direction_id, shape_id)
        SELECT route_id, service_id, trip_id, direction_id, shape_id FROM Trips_VIRT;",
    )?;

    // Let's try doing type affinity conversions for the big boi

    // Experiment: if it's a covering index why not just create it on the virtual table?
    // Result: Virtual tables can't be indexed.
    // Experiment: if we can create the table with a primary key in the first place, maybe we can save a scan later?
    // Result: No. Faster to populate the table and create the index after. No FKs either.
    //      (It does take up way more memory though.)
    db.execute_batch("CREATE TABLE StopTimes (trip_id TEXT, stop_id INT, stop_sequence INT);")?;
    db.execute_batch(
        "INSERT INTO StopTimes (trip_id, stop_id, stop_sequence)
    SELECT trip_id, stop_id, stop_sequence FROM StopTimes_VIRT;",
    )?;
    db.execute_batch("CREATE INDEX idx_stoptimes ON StopTimes(trip_id, stop_id, stop_sequence)")?;

    // eprintln!(
    //     "Info: GTFS actual tables (incl. StopTimes index) at {} ms.",
    //     now.elapsed().as_millis()
    // );

    // Also create some helper views
    /*
        db.execute_batch(
            "CREATE VIEW TripSeqs AS
            SELECT StopTimes.stop_id, StopTimes.stop_sequence, Trips.direction_id, Trips.route_id, Trips.shape_id
            FROM StopTimes
            INNER JOIN Trips on StopTimes.trip_id = Trips.trip_id;",
        )?;

        db.execute_batch(
            "CREATE VIEW TripSeqCounts(stop_id, stop_sequence, direction_id, route_id, shape_id, qty)
            AS SELECT stop_id, stop_sequence, direction_id, route_id, shape_id, Count(*)
            FROM TripSeqs
            GROUP BY stop_id, stop_sequence, direction_id, route_id, shape_id;",
        )?;

        // unfortunately different route variations have the SAME route_id
        // see e.g. the 397 Inbound
        // so we have to use shape_id, which is a bit hacky

        // basically I just find VIEWS easiest to reason about, so we're creating
        // an intermediate view SSI, and then the table StopSeqs

        db.execute_batch(
            "CREATE VIEW SSI (stop_id, stop_sequence, direction_id, route_short_name, shape_id, qty)
            AS SELECT stop_id, stop_sequence, direction_id, route_short_name, shape_id, SUM(qty)
            FROM TripSeqCounts
            INNER JOIN Routes ON TripSeqCounts.route_id = Routes.route_id
            GROUP BY stop_id, stop_sequence, direction_id, route_short_name, shape_id;",
        )?;
    */

    /* *** new way */

    db.execute_batch("CREATE VIEW RSC AS SELECT route_short_name, direction_id, shape_id, count(trip_id) as qty, min(trip_id) as trip_id 
    FROM Trips INNER JOIN Routes ON Trips.route_id = Routes.route_id 
    GROUP BY route_short_name, direction_id, shape_id;")?;

    db.execute_batch("CREATE TABLE RouteShapeCounts AS SELECT * FROM RSC;")?;

    db.execute_batch("CREATE VIEW SSI (stop_id, stop_sequence, direction_id, route_short_name, shape_id, qty) AS 
    select S.stop_id, S.stop_sequence, R.direction_id, R.route_short_name, R.shape_id, R.qty 
    FROM RouteShapeCounts R, StopTimes S WHERE R.trip_id = S.trip_id;")?;

    /* *** end of new way *** */

    // pre-chewing this table in particular makes subsequent queries much faster
    // SQLite doesn't have materialized views, so this is effectively that
    // as we should be read-only once load_gtfs returns
    db.execute_batch("CREATE TABLE StopSeqs AS SELECT * FROM SSI;")?;
    // However this alone takes almost half the runtime now

    // And indexing it makes things faster still:
    db.execute_batch("CREATE INDEX ss_routedir ON StopSeqs(route_short_name, direction_id);")?;
    db.execute_batch("CREATE INDEX ss_shapeid ON StopSeqs(shape_id);")?;

    // eprintln!(
    //     "Info: GTFS StopSeqs and indexes at {} ms.",
    //     now.elapsed().as_millis()
    // );

    // Now we are preparing to approximate service levels
    // GTFS is built around the concept of a service day; StopTimes only has times, not dates
    // So a service_id might represent "weekdays" or "weekends" or "Monday - Thursday"

    // RTI and then RTF connect route/directions to service_ids...
    db.execute_batch(
        "CREATE VIEW RTI (trip_id, service_id, route_short_name, direction_id)
        AS SELECT trip_id, service_id, route_short_name, direction_id
        FROM Trips INNER JOIN Routes ON Routes.route_id = Trips.route_id;",
    )?;

    // aggregating over trip_ids..
    db.execute_batch(
        "CREATE VIEW RTF (service_id, route_short_name, direction_id, freq)
        AS select service_id, route_short_name, direction_id, count(*)
        FROM RTI GROUP BY service_id, route_short_name, direction_id;",
    )?;

    // denormalising over service_ids...
    db.execute_batch("CREATE VIEW SDI (route_short_name, direction_id, freq, monday, tuesday, wednesday, thursday, friday, saturday, sunday)
    AS SELECT route_short_name, direction_id, freq, monday, tuesday, wednesday, thursday, friday, saturday, sunday
    FROM RTF INNER JOIN Calendar on Calendar.service_id = RTF.service_id;")?;

    // pre-chew everything again
    db.execute_batch(
        "CREATE TABLE ServiceCounts (route_short_name TEXT, direction_id TEXT, freq INTEGER, 
        monday INTEGER, tuesday INTEGER, wednesday INTEGER, 
        thursday INTEGER, friday INTEGER, saturday INTEGER, sunday INTEGER);",
    )?;
    db.execute_batch("INSERT INTO ServiceCounts SELECT * FROM SDI;")?;

    /*
    let mut stmt = db.prepare("SELECT count(*) from RTI;")?;
    let servicecounts: i32 = stmt.query_row([], |r| r.get(0))?;
    eprintln!("Test: RTI view contains {} rows.", servicecounts);

    stmt = db.prepare("SELECT count(*) from RTF;")?;
    let servicecounts: i32 = stmt.query_row([], |r| r.get(0))?;
    eprintln!("Test: RTF view contains {} rows.", servicecounts);

    stmt = db.prepare("SELECT count(*) from SDI;")?;
    let servicecounts: i32 = stmt.query_row([], |r| r.get(0))?;
    eprintln!("Test: SDI view contains {} rows.", servicecounts);

    stmt = db.prepare("SELECT count(*) from ServiceCounts;")?;
    let servicecounts: i32 = stmt.query_row([], |r| r.get(0))?;
    eprintln!("Test: ServiceCounts table contains {} rows.", servicecounts);
    */

    // eprintln!("Info: GTFS done at {} ms.", now.elapsed().as_millis());

    Ok(())
}

// currently unused
/*
fn get_gtfs_routelist(db: &Connection) -> Result<Vec<String>, rusqlite::Error> {
    //! Get all the routes that *GTFS* knows about
    let mut stmt = db
        .prepare("SELECT route_short_name FROM Routes")
        .expect("Failed preparing statement");

    let x = Ok(stmt
        .query_map(NO_PARAMS, |r| r.get(0))?
        .filter_map(|r| r.ok())
        .collect());
    x
}
*/

#[inline(never)]
fn get_gtfs_stop_seqs(
    db: &Connection,
    route: &str,
    direction: &str,
) -> Result<Vec<StopSeq>, serde_rusqlite::Error> {
    //! Get the `StopSeqs` for all services of the given route/direction.
    //! Route variations are distinguishable by `shape_id`.

    // Frustrating that we have to use shape_id rather than route_id...

    let mut stmt = db.prepare(
        "SELECT stop_id, stop_sequence, shape_id, qty
        FROM StopSeqs WHERE route_short_name IS :route AND direction_id IS :direction
        ORDER BY shape_id, stop_sequence;",
    )?;

    let out = from_rows::<StopSeq>(stmt.query(&[(":route", &route), (":direction", &direction)])?)
        .collect();
    out
}

#[inline(never)]
fn get_gtfs_first_lasts(
    db: &Connection,
    route: &str,
    direction: &str,
) -> Result<Vec<FirstLastSeq>, serde_rusqlite::Error> {
    let mut stmt = db.prepare(
        "SELECT A.stop_id as first, B.stop_id as last, A.shape_id, B.stop_sequence as len, A.qty
        FROM StopSeqs A, StopSeqs B 
        WHERE A.stop_sequence = 1 AND A.shape_id = B.shape_id 
        AND A.route_short_name IS :route AND A.direction_id IS :direction
        GROUP BY A.shape_id HAVING MAX(B.stop_sequence) ORDER BY A.stop_id;",
    )?;

    let out =
        from_rows::<FirstLastSeq>(stmt.query(&[(":route", &route), (":direction", &direction)])?)
            .collect();
    out
}

#[inline(never)]
pub fn make_stop_sequence(
    db: &Connection,
    route: &str,
    direction_name: &str,
) -> Result<Vec<i64>, serde_rusqlite::Error> {
    //! Creates a route-ordered list of `stop_id`s for a given route/direction.

    /* This task is actually rather complicated:

      * We commence with a number of of different paths which represent an individual services' run
        * (These are paths in the sense that they're a sequence of discrete points)
      * These paths are ordered within themselves but other than that they can do anything
        * some might be subsequences of another,
        * some might have some parts in common but others conflicting,
        * some might end with anothers' start...
      * We will attempt to adhere to a loose topological ordering here:
        * our output should be a toposort if the paths are a DAG overall
        * if there is a simple major cycle then we choose a node to split at
          * (i.e. if all paths were split at a node, the result would be a DAG)
        * cross fingers that there's nothing more complicated
    */

    let direction = convert_direction(direction_name);

    let rows = match get_gtfs_stop_seqs(db, route, direction) {
        Ok(o) => o,
        Err(e) => return Err(e),
    };
    //     eprintln!("Executed GTFS query OK! {} rows returned...", rows.len());

    if rows.is_empty() {
        return Err(serde_rusqlite::Error::Rusqlite(
            rusqlite::Error::QueryReturnedNoRows,
        ));
    }

    /* Oracling like a boss
     * Pragma: only stops that a run starts on should be a "start stop"
     * and any stops that have no prior stops in the graph beat all others
     * if there's a cycle that contains *every* stop, then the stop with the
     * plurality of starts should be selected
     * tiebreak by boardings, then stop_id
     */

    let mut firsts: BTreeMap<i64, i64> = BTreeMap::new();
    let mut not_only_firsts: HashSet<i64> = HashSet::new();
    let mut shape_stops: BTreeMap<&str, i64> = BTreeMap::new();
    let mut first_stop_shapes: BTreeMap<i64, &str> = BTreeMap::new();

    //     println!("ID\tSeq.\tShape\tQty");

    for r in &rows {
        //         println!(
        //             "{}\t{}\t{}\t{}",
        //             r.stop_id, r.stop_sequence, r.shape_id, r.qty
        //         );

        if r.stop_sequence < 2 {
            let c: i64 = *firsts.get(&r.stop_id).unwrap_or(&0);
            firsts.insert(r.stop_id, r.qty + c);
            shape_stops.insert(&r.shape_id, r.stop_id);
            first_stop_shapes.insert(r.stop_id, &r.shape_id);
        } else {
            not_only_firsts.insert(r.stop_id);
        }
    }

    // Ideally we want a "pure" first but they aren't always available.
    // Sort all firsts by runs and then patronage
    let mut only_firsts: Vec<(i64, i64, i64)> = Vec::new();
    let mut all_firsts: Vec<(i64, i64, i64)> = Vec::new();
    for (id, firsts) in &firsts {
        let patronage = get_boardings(db, route, direction_name, *id).unwrap_or(0);
        all_firsts.push((*firsts, patronage, *id));
        if !not_only_firsts.contains(id) {
            only_firsts.push((*firsts, patronage, *id));
        }
    }
    all_firsts.sort_unstable();
    all_firsts.reverse();
    only_firsts.sort_unstable();
    only_firsts.reverse();

    let oracle_stop_id = match only_firsts.len() {
        0 => all_firsts.first().unwrap().2,
        _ => only_firsts.first().unwrap().2,
    };
    // If there was a pure-first we've picked it to be oracle_stop_id
    // Otherwise we've taken the best non-pure first

    //     println!("\n\nStarting stop_id: {:?}", oracle_stop_id);

    // We have a good, but not great, consideration ordering of stop sequences
    // *Extremely* cheeky solution: go by physical closeness to last stop

    // collate as-yet unallocated sequence starts
    let mut unalloc: HashSet<i64> = all_firsts
        .iter()
        .filter_map(|r| {
            if r.2 != oracle_stop_id {
                Some(r.2)
            } else {
                None
            }
        })
        .collect();
    let mut final_order: Vec<i64> = Vec::with_capacity(all_firsts.len());
    let mut prev_first: i64 = oracle_stop_id;

    // get a lookup table of first and last stop_ids pre-sorted by first
    let first_last_rows: Vec<FirstLastSeq> = match get_gtfs_first_lasts(db, route, direction) {
        Ok(o) => o,
        Err(e) => return Err(e),
    };
    if first_last_rows.is_empty() {
        return Err(serde_rusqlite::Error::Rusqlite(
            rusqlite::Error::QueryReturnedNoRows,
        ));
    }

    // physical-closeness iteration
    for _ in 0..all_firsts.len() {
        final_order.push(prev_first);

        let prev_idx = first_last_rows
            .binary_search_by_key(&prev_first, |x| x.first)
            .unwrap();
        let prev_last = first_last_rows[prev_idx].last;

        // need lat/long of prev_last
        let mut stmt =
            db.prepare("SELECT stop_lat, stop_lon FROM Stops WHERE stop_id = :stop_id;")?;
        let prev_coords = stmt.query_row(&[(":stop_id", &prev_last)], |r| {
            Ok((r.get_unwrap(0), r.get_unwrap(1)))
        })?;

        let prev_lat: f64 = prev_coords.0;
        let prev_lon: f64 = prev_coords.1;

        let mut min_dist = f64::MAX;
        let mut min_dist_k = prev_first;

        // iterate over as-yet-unallocated sequence starts and select closest
        for k in &unalloc {
            let test_coords = stmt.query_row(&[(":stop_id", &k)], |r| {
                Ok((r.get_unwrap(0), r.get_unwrap(1)))
            })?;
            let test_lat: f64 = test_coords.0;
            let test_lon: f64 = test_coords.1;
            let dist = gc_distance(prev_lat, prev_lon, test_lat, test_lon);
            if dist < min_dist {
                min_dist = dist;
                min_dist_k = *k;
            }
        }
        prev_first = min_dist_k;
        unalloc.remove(&min_dist_k);
    }

    //     println!("{:?}", final_order);

    // now create a deque of deques for topomerging

    let mut mainde: VecDeque<VecDeque<i64>> = VecDeque::new();

    for stop in &final_order {
        for shape in shape_stops.iter().filter_map(|(k, v)| match v == stop {
            true => Some(k),
            _ => None,
        }) {
            let mut de = VecDeque::new();
            for r in &rows {
                if r.shape_id == *shape {
                    de.push_back(r.stop_id)
                }
            }
            mainde.push_back(de);
        }
    }

    //     println!("mainde: {:?}", mainde);

    Ok(topo_merge(mainde))
}

fn topo_merge(mut input: VecDeque<VecDeque<i64>>) -> Vec<i64> {
    //! Merge a collection of ordered sequences in a toposort-compatible way

    //! * input: a collection of sequences of of `stop_ids` (in `stop_sequence` order), one per `shape_id`  
    //! * output: a single, merged sequence of `stop_ids`  

    //! ```text
    //! loop over sequences:    
    //!   loop over contents:  
    //!     pop a stop off the front,
    //!     if it's on the front of any others, pop them too (result: merge)
    //!     if temp queue already contains the stop, append queue contents to output; loop
    //!     else if output already contains stop_id, insert the contents of the temp queue
    //!     immediately prior to that point in the output (retaining original ordering)
    //!     otherwise push current stop_id on the temp queue
    //!   at the end of the current sequence, append temp queue to the output
    //!   clear temp queue, iterate
    //! ```

    let mut output: Vec<i64> = Vec::new();
    // a temporary queue
    let mut temp: VecDeque<i64> = VecDeque::new();

    while !input.is_empty() {
        let mut de = input.pop_front().unwrap();
        while !de.is_empty() {
            let id = de.pop_front().unwrap();
            // if the temp queue already contains this stop, we have a loop to break
            // solution: cut and run
            if temp.contains(&id) {
                for t in &temp {
                    output.push(*t);
                }
                temp.clear();
                continue;
            }
            // "merge" other sequences in if possible (by popping this stop from them)
            for ode in &mut input {
                if let Some(o) = ode.front() {
                    if *o == id {
                        ode.pop_front();
                    }
                }
            }
            // if this stop is already in the output, insert temp queue prior to it
            if let Some(c) = output.iter().position(|s| *s == id) {
                let mut cursor = c;
                //                     println!("... found duplicate {} at {}", id, c);
                for t in &temp {
                    output.insert(cursor, *t);
                    cursor += 1;
                }
                temp.clear();
            } else {
                // nothing else for it: append this stop to the temp queue
                temp.push_back(id);
            }
        }
        // end of the sequence, append any remaining temp queue to output
        for t in &temp {
            output.push(*t);
        }
        temp.clear();
    }

    output
}

fn gc_distance(from_lat: f64, from_lon: f64, to_lat: f64, to_lon: f64) -> f64 {
    //! Calculate the great-circle distance between two points on Earth.
    //! Takes coordinates in decimal degrees.

    let r = 6_371_000.0; // approximate average radius of Earth

    // radian conversion
    let from_lat = from_lat * std::f64::consts::PI / 180.0;
    let from_lon = from_lon * std::f64::consts::PI / 180.0;
    let to_lat = to_lat * std::f64::consts::PI / 180.0;
    let to_lon = to_lon * std::f64::consts::PI / 180.0;

    // Inverse Haversine formula
    2.0 * r
        * ((((to_lat - from_lat) / 2.0).sin().powi(2)
            + from_lat.cos() * to_lat.cos() * (((to_lon - from_lon) / 2.0).sin().powi(2)))
        .sqrt()
        .asin())
}

pub fn get_stop_names(
    db: &Connection,
    input: &[i64],
) -> Result<BTreeMap<i64, String>, serde_rusqlite::Error> {
    //! Get stop names from stop sequences
    let mut output: BTreeMap<i64, String> = BTreeMap::new();

    let mut stmt = db.prepare_cached("SELECT stop_name FROM Stops WHERE stop_id = :id")?;

    for id in input {
        let name: String = stmt.query_row(&[(":id", &id)], |r| r.get(0))?;
        output.insert(*id, name);
    }

    Ok(output)
}

pub fn get_service_count(
    db: &Connection,
    route: &str,
    direction_name: &str,
    month: &str,
    year: &str,
) -> serde_rusqlite::Result<i32> {
    //! Get the (estimated) monthly service count for the specified route/direction.

    let mut stmt = db.prepare(
        "SELECT freq, monday, tuesday, wednesday, thursday, friday, saturday, sunday
    FROM ServiceCounts WHERE route_short_name = :route AND direction_id = :direction",
    )?;

    let res = from_rows::<ServiceCounts>(stmt.query(&[
        (":route", &route),
        (":direction", &convert_direction(direction_name)),
    ])?);

    // Problem: often there are a few different trip_ids. They're for different date ranges.
    // We need to disambiguate by selecting only one trip_id per day of the week
    // Proposed method: use the max freq for any given day

    let mut maxes = vec![0_i32; 7];

    // eprintln!("\n{} {}", route, direction_name);

    for row in res {
        let r = row?;

        // eprintln!("{:#?}", r);

        if r.monday > 0 && maxes[0] < r.freq {
            maxes[0] = r.freq;
        }
        if r.tuesday > 0 && maxes[1] < r.freq {
            maxes[1] = r.freq;
        }
        if r.wednesday > 0 && maxes[2] < r.freq {
            maxes[2] = r.freq;
        }
        if r.thursday > 0 && maxes[3] < r.freq {
            maxes[3] = r.freq;
        }
        if r.friday > 0 && maxes[4] < r.freq {
            maxes[4] = r.freq;
        }
        if r.saturday > 0 && maxes[5] < r.freq {
            maxes[5] = r.freq;
        }
        if r.sunday > 0 && maxes[6] < r.freq {
            maxes[6] = r.freq;
        }
    }

    let out: i32 =
        (maxes.iter().sum::<i32>() as f32 * days_per_month(month, year) / 7.0).round() as i32;
    Ok(out)
}
