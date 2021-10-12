//! Functions for dealing with GTFS...

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::iter::Iterator;
use std::path::PathBuf;

use rusqlite::{Connection, Result};

use serde::{Deserialize, Serialize};
use serde_rusqlite::*;

use super::*;

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

pub fn load_gtfs(db: &Connection, mut dir: PathBuf) -> Result<(), rusqlite::Error> {
    //! Loads all the GTFS CSVs into SQLite tables in `db`

    for (t, p) in [
        ("Calendar", "calendar.txt"),
        ("Routes", "routes.txt"),
        ("Stops", "stops.txt"),
        ("StopTimes", "stop_times.txt"),
        ("Trips", "trips.txt"),
    ]
    .iter()
    {
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

    // pre-creating tables is what makes this perform at a reasonable speed
    db.execute_batch("CREATE TABLE Calendar AS SELECT * FROM Calendar_VIRT;")?;
    db.execute_batch("CREATE TABLE Routes AS SELECT * FROM Routes_VIRT;")?;
    db.execute_batch(
        "CREATE TABLE Stops (stop_id INT, stop_name TEXT, stop_lat REAL, stop_lon REAL);",
    )?;
    db.execute_batch(
        "INSERT INTO Stops (stop_id, stop_name, stop_lat, stop_lon) SELECT stop_id, stop_name, stop_lat, stop_lon FROM Stops_VIRT;",
    )?;
    db.execute_batch("CREATE TABLE Trips AS SELECT * FROM Trips_VIRT;")?;
    // Let's try doing type affinity conversions for the big boi
    db.execute_batch("CREATE TABLE StopTimes (trip_id TEXT, stop_id INT, stop_sequence INT);")?;
    db.execute_batch(
        "INSERT INTO StopTimes (trip_id, stop_id, stop_sequence)
    SELECT trip_id, stop_id, stop_sequence FROM StopTimes_VIRT;",
    )?;

    // Also create some helper views

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

    // pre-chewing this table in particular makes subsequent queries lightning-fast
    db.execute_batch("CREATE TABLE StopSeqs AS SELECT * FROM SSI;")?;

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
    db.execute_batch("CREATE TABLE ServiceCounts (route_short_name TEXT, direction_id TEXT, freq INTEGER, monday INTEGER, tuesday INTEGER, wednesday INTEGER, thursday INTEGER, friday INTEGER, saturday INTEGER, sunday INTEGER);")?;
    db.execute_batch("INSERT INTO ServiceCounts SELECT * FROM SDI;")?;

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

fn get_gtfs_stop_seqs(
    db: &Connection,
    route: &str,
    direction: &str,
) -> Result<Vec<StopSeq>, serde_rusqlite::Error> {
    //! Get the StopSeqs for all services of the given route/direction.
    //! Route variations are distinguishable by `shape_id`.

    // Frustrating that we have to use shape_id rather than route_id...

    let mut stmt = db.prepare(
        "SELECT stop_id, stop_sequence, shape_id, qty
        FROM StopSeqs WHERE route_short_name IS :route AND direction_id IS :direction
        ORDER BY shape_id, stop_sequence;",
    )?;

    let out =
        from_rows::<StopSeq>(stmt.query_named(&[(":route", &route), (":direction", &direction)])?)
            .collect();
    out
}

fn get_prev_last(
    db: &Connection,
    route: &str,
    direction: &str,
    prev_first: i64,
) -> Result<i64, rusqlite::Error> {
    //! Get the last stop_id in any sequence for a given first stop_id
    let mut stmt = db.prepare("SELECT MAX(stop_sequence) FROM StopSeqs WHERE shape_id IN 
    (SELECT shape_id FROM StopSeqs WHERE route_short_name = :route AND direction_id = :direction AND stop_id = :prev_first);")?;

    let maxi: i64 = stmt.query_row_named(
        &[
            (":route", &route),
            (":direction", &direction),
            (":prev_first", &prev_first),
        ],
        |row| row.get(0),
    )?;

    let mut stmt = db.prepare("SELECT stop_id FROM StopSeqs WHERE stop_sequence = :maxi AND shape_id IN 
    (SELECT shape_id FROM StopSeqs WHERE route_short_name = :route AND direction_id = :direction AND stop_id = :prev_first);")?;

    stmt.query_row_named(
        &[
            (":maxi", &maxi),
            (":route", &route),
            (":direction", &direction),
            (":prev_first", &prev_first),
        ],
        |row| row.get(0),
    )
}

pub fn make_stop_sequence(
    db: &Connection,
    route: &str,
    direction_name: &str,
) -> Result<Vec<i64>, serde_rusqlite::Error> {
    //! Creates a route-ordered list of `stop_id`s for a given route/direction.

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

    //     println!("ID\tSeq.\tShape\tQty");

    for r in rows.iter() {
        //         println!(
        //             "{}\t{}\t{}\t{}",
        //             r.stop_id, r.stop_sequence, r.shape_id, r.qty
        //         );

        if r.stop_sequence < 2 {
            let c: i64 = *firsts.get(&r.stop_id).unwrap_or(&0);
            firsts.insert(r.stop_id, r.qty + c);
            shape_stops.insert(&r.shape_id, r.stop_id);
        } else {
            not_only_firsts.insert(r.stop_id);
        }
    }

    // Ideally we want a "pure" first but they aren't always available.
    // Sort all firsts by runs and then patronage
    let mut only_firsts: Vec<(i64, i64, i64)> = Vec::new();
    let mut all_firsts: Vec<(i64, i64, i64)> = Vec::new();
    for (id, firsts) in firsts.iter() {
        let patronage = get_boardings(db, route, direction_name, *id).unwrap_or(0);
        all_firsts.push((*firsts, patronage, *id));
        if !not_only_firsts.contains(id) {
            only_firsts.push((*firsts, patronage, *id));
        }
    }
    all_firsts.sort();
    all_firsts.reverse();
    only_firsts.sort();
    only_firsts.reverse();

    let oracle_stop_id = match only_firsts.len() {
        0 => all_firsts.first().unwrap().2,
        _ => only_firsts.first().unwrap().2,
    };

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

    // physical-closeness iteration
    for _ in 0..all_firsts.len() {
        final_order.push(prev_first);

        let prev_last = get_prev_last(db, route, direction, prev_first).unwrap();

        // need lat/long of prev_last
        let mut stmt =
            db.prepare("SELECT stop_lat, stop_lon FROM Stops WHERE stop_id = :stop_id;")?;
        let prev_coords = stmt.query_row_named(&[(":stop_id", &prev_last)], |r| {
            Ok((r.get_unwrap(0), r.get_unwrap(1)))
        })?;

        let prev_lat: f64 = prev_coords.0;
        let prev_lon: f64 = prev_coords.1;

        let mut min_dist = f64::MAX;
        let mut min_dist_k = prev_first;

        // iterate over as-yet-unallocated sequence starts and select closest
        for k in &unalloc {
            let test_coords = stmt.query_row_named(&[(":stop_id", &k)], |r| {
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

    for stop in final_order.iter() {
        for shape in shape_stops.iter().filter_map(|(k, v)| match v == stop {
            true => Some(k),
            _ => None,
        }) {
            let mut de = VecDeque::new();
            for r in rows.iter() {
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

    //! * input: a collection of sequences of of stop_ids (in stop_sequence order), one per shape_id  
    //! * output: a single, merged sequence of stop_ids  

    //! ```
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
                for t in temp.iter() {
                    output.push(*t);
                }
                temp.clear();
                continue;
            }
            // "merge" other sequences in if possible (by popping this stop from them)
            for ode in input.iter_mut() {
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
                for t in temp.iter() {
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
        for t in temp.iter() {
            output.push(*t);
        }
        temp.clear();
    }

    output
}

fn gc_distance(from_lat: f64, from_lon: f64, to_lat: f64, to_lon: f64) -> f64 {
    //! Calculate the great-circle distance between two points on Earth.
    //! Takes coordinates in decimal degrees.

    let r = 6371000.0; // approximate average radius of Earth

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
        let name: String = stmt.query_row_named(&[(":id", &id)], |r| r.get(0))?;
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

    let res = from_rows::<ServiceCounts>(stmt.query_named(&[
        (":route", &route),
        (":direction", &convert_direction(direction_name)),
    ])?);

    // Problem: often there are a few different trip_ids. They're for different date ranges.
    // We need to disambiguate by selecting only one trip_id per day of the week
    // Proposed method: use the max freq for any given day

    let mut maxes = vec![0_i32; 7];

    for row in res {
        let r = row?;
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

    let out: i32 = maxes.iter().sum();
    Ok((out as f32 * days_per_month(month, year) / 7.0).round() as i32)
}
