use rusqlite::{params, Connection, Result, NO_PARAMS};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::iter::{FilterMap, Iterator};
use std::path::PathBuf;

use std::process::exit;
use std::time::Instant;

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

pub fn load_gtfs(db: &Connection, mut dir: PathBuf) -> Result<(), rusqlite::Error> {
    //! Loads all the GTFS CSVs into SQLite tables in `db`
    eprintln!("Loading GTFS. This may take several seconds...");

    for (t, p) in [
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

    let now = Instant::now();

    // pre-creating tables is what makes this perform at a reasonable speed
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
    // an intermediate view SSI, ~a~final~view~SSF~ and then the table StopSeqs

    db.execute_batch(
        "CREATE VIEW SSI (stop_id, stop_sequence, direction_id, route_short_name, shape_id, qty)
        AS SELECT stop_id, stop_sequence, direction_id, route_short_name, shape_id, SUM(qty)
        FROM TripSeqCounts
        INNER JOIN Routes ON TripSeqCounts.route_id = Routes.route_id
        GROUP BY stop_id, stop_sequence, direction_id, route_short_name, shape_id;",
    )?;

    //     db.execute_batch(
    //         "CREATE VIEW SSF (stop_name, stop_sequence, direction_id, route_short_name, shape_id, qty)
    //         AS SELECT Stops.stop_name, SSI.stop_sequence, SSI.direction_id, SSI.route_short_name, SSI.shape_id, SSI.qty
    //         FROM SSI
    //         INNER JOIN Stops ON Stops.stop_id = SSI.stop_id
    //         GROUP BY stop_sequence, direction_id, route_short_name;",
    //     )?;

    // pre-chewing this table in particular makes subsequent queries lightning-fast
    db.execute_batch("CREATE TABLE StopSeqs AS SELECT * FROM SSI;")?;

    eprintln!(
        "Loaded and preprocessed GTFS in {} seconds",
        now.elapsed().as_secs()
    );

    Ok(())
}

fn get_gtfs_routelist(db: &Connection) -> Result<Vec<String>, rusqlite::Error> {
    let mut stmt = db
        .prepare("SELECT route_short_name FROM Routes")
        .expect("Failed preparing statement");

    let x = Ok(stmt
        .query_map(NO_PARAMS, |r| r.get(0))?
        .filter_map(|r| r.ok())
        .collect());
    x
}

fn get_gtfs_stop_seqs(
    db: &Connection,
    route: &str,
    direction: &str,
) -> Result<Vec<StopSeq>, serde_rusqlite::Error> {
    //! Get the StopSeqs for all services of the given route/direction.
    //! Frustratingly, TransLink uses the same route_id for different route variations
    //! so if we don't use the raw data out of StopTimes then we only have the counts for disambiguation.
    //! In practice, qty collisions seem rare.
    //! **However...** the `shape_id`s are also distinct per-variation.

    let schema = format!(
        "SELECT stop_id, stop_sequence, shape_id, qty
        FROM StopSeqs WHERE route_short_name IS {} AND direction_id IS {}
        ORDER BY shape_id, stop_sequence;",
        route, direction
    );

    let mut stmt = db.prepare(&schema).unwrap();
    let out = from_rows::<StopSeq>(stmt.query(NO_PARAMS)?).collect();
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
    //! Turns `get_gtfs_stop_seqs`' output into what we really want: an ordered list of stops
    //! ready for display

    /* Walking the stop sequences
     * start at the sources [sequence = 1]
     * go through until hit a multinode
     * start from next source
     * then at that multinode, all of the non-multinode nexts are new sources
     * repeat
     * that's probably almost a toposort but it doesn't entirely solve the loop problem
     * oracle something to be the last stop
     */

    // Longest-common-subsequence has also been recommended

    // but for now, what will suffice is to oracle something to be the last stop,
    // cut the loop there, and then do a toposort via DFW

    let direction = convert_direction(direction_name);

    let rows = match get_gtfs_stop_seqs(db, route, direction) {
        Ok(o) => o,
        Err(e) => return Err(e),
    };
//     eprintln!("Executed GTFS query OK! {} rows returned...", rows.len());

    if rows.len() == 0 {
        return Err(serde_rusqlite::Error::Rusqlite(
            rusqlite::Error::QueryReturnedNoRows,
        ));
    }

    /* Oracling like a boss
     * Pragma: only stops that a run starts on should be a start stop
     * and any stops that have no prior stops in the graph beat all others
     * if there's a cycle that contains *every* stop, then the stop with the
     * plurality of starts should be selected
     * tiebreak by boardings, then stop_id
     */

    let mut oracle_firsts: BTreeMap<i64, i64> = BTreeMap::new();
    let mut not_only_firsts: HashSet<i64> = HashSet::new();
    let mut shape_stops: BTreeMap<&str, i64> = BTreeMap::new();

//     println!("ID\tSeq.\tShape\tQty");

    struct StopItem {
        stop_sequence: i64,
        shape_id: String,
        qty: i64,
    }

    //     let mut stop_items: BTreeMap<(i64, StopItem)>  = BTreeMap::new();

    for r in rows.iter() {
//         println!(
//             "{}\t{}\t{}\t{}",
//             r.stop_id, r.stop_sequence, r.shape_id, r.qty
//         );

        if (r.stop_sequence < 2) {
            let c: i64 = *oracle_firsts.get(&r.stop_id).unwrap_or(&0);
            oracle_firsts.insert(r.stop_id, r.qty + c);
            shape_stops.insert(&r.shape_id, r.stop_id);
        } else {
            not_only_firsts.insert(r.stop_id);
        }
    }

    // we want to select a "pure" first, possibly from one of several
    // and if one isn't available, than from a not_only_first

    let only_firsts: Vec<i64> = oracle_firsts
        .keys()
        .cloned()
        .filter(|x| !not_only_firsts.contains(x))
        .collect();

    // life can be messy (or loopy). Sort all firsts by runs and then patronage
    let mut onlyfirsts: Vec<(i64, i64, i64)> = Vec::new();
    let mut allfirstslist: Vec<(i64, i64, i64)> = Vec::new();
    for (id, firsts) in oracle_firsts.iter() {
        let patronage = get_boardings(db, route, direction_name, *id).unwrap_or(0);
        allfirstslist.push((*firsts, patronage, *id));
        if !not_only_firsts.contains(id) {
            onlyfirsts.push((*firsts, patronage, *id));
        }
    }

    allfirstslist.sort();
    allfirstslist.reverse();
    onlyfirsts.sort();
    onlyfirsts.reverse();

    let oracle_stop_id = match onlyfirsts.len() {
        0 => allfirstslist.first().unwrap().2,
        _ => onlyfirsts.first().unwrap().2,
    };

//     println!("\n\nStarting stop_id: {:?}", oracle_stop_id);

    // need to order all the pure firsts (already done, arbitrarily)
    // and then all the not_only_firsts
    // extremely cheeky solution: go by physical closeness to last stop

    let mut unalloc: HashSet<i64> = allfirstslist
        .iter()
        .filter_map(|r| {
            if r.2 != oracle_stop_id {
                Some(r.2)
            } else {
                None
            }
        })
        .collect();
    let mut final_order: Vec<i64> = Vec::with_capacity(allfirstslist.len());
    let mut prev_first: i64 = oracle_stop_id;

    for _ in 0..allfirstslist.len() {
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

    // now create a deque of deques

    let mut mainde: VecDeque<VecDeque<i64>> = VecDeque::new();

    for stop in final_order.iter() {
        for shape in shape_stops.iter().filter_map(|(k, v)| match v == stop {
            true => Some(k),
            _ => None,
        }) {
            let mut de = VecDeque::new();
            for r in rows.iter() {
                if &r.shape_id == *shape {
                    de.push_back(r.stop_id)
                }
            }
            mainde.push_back(de);
        }
    }

//     println!("mainde: {:?}", mainde);

    Ok(topomerge(mainde))
}

fn topomerge(mut input: VecDeque<VecDeque<i64>>) -> Vec<i64> {
    //! Merge a collection of ordered sequences in a toposort-compatible way

    //! * input: a collection of sequences of of stop_ids (in stop_sequence order), one per shape_id  
    //! * output: a single sequence of stop_ids  
    //! * temp: deque of stop_ids  

    //! ```
    //! loop over sequences:    
    //!   loop over contents:  
    //!     pop a stop off the front,
    //!     if it's on the front of any others, pop them too (result: merge)
    //!     if output already contains stop_id, insert the contents of the temp queue
    //!     immediately prior to that point in the output (retaining original ordering)
    //!     otherwise push current stop_id on the temp queue
    //!     if hit the end of the current sequence (and output doesn't already contain
    //!     current stop_id) then append temp queue to the output (again, retain ordering)
    //!     (clear temp queue, iterate)
    //! ```

    let mut output: Vec<i64> = Vec::new();
    let mut temp: VecDeque<i64> = VecDeque::new();

    while !input.is_empty() {
        let mut de = input.pop_front().unwrap();
        //         match de.front() {
        //             Some(o) => println!("New sequence starting with {}", o),
        //             None => println!("Uh-oh - empty sequence!")
        //         };

        while !de.is_empty() {
            if let Some(id) = de.pop_front() {
                //                 println!("{}", id);

                if temp.contains(&id) {
                    for t in temp.iter() {
                        output.push(*t);
                    }
                    temp.clear();
                    continue;
                }

                for ode in input.iter_mut() {
                    if let Some(o) = ode.front() {
                        if *o == id {
                            ode.pop_front();
                        }
                    }
                }
                if let Some(c) = output.iter().position(|s| *s == id) {
                    let mut cursor = c;
                    //                     println!("... found duplicate {} at {}", id, c);
                    for t in temp.iter() {
                        output.insert(cursor, *t);
                        cursor = cursor + 1;
                    }
                    temp.clear();
                //                     println!("... drained queue")
                } else {
                    temp.push_back(id);
                    //                     println!("... pushed {}", id);
                }
            }
        }
        for t in temp.iter() {
            output.push(*t);
        }
        temp.clear();
        //         println!("drained main queue")
    }

    return output;
}

fn gc_distance(from_lat: f64, from_lon: f64, to_lat: f64, to_lon: f64) -> f64 {
    //! Calculate the distance between two Coordinates as a
    //! great-circle distance on the Earth.
    //! Takes coordinates in decimal degrees for convenience

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
