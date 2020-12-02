//! Fluvial
//!
//! A bit like a Sankey diagram, only a little simpler.
//! Intended for visualising passenger flows over a route.

extern crate hsluv;
#[macro_use]
extern crate clap;

use clap::{App, Arg, ArgGroup};
use core::fmt::Display;
use hsluv::*;
use rand::Rng;

use rusqlite::{params, Connection, Result, NO_PARAMS};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::iter::{FilterMap, Iterator};
use std::path::PathBuf;

use std::process::exit;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_rusqlite::*;

mod gtfs;
use crate::gtfs::*;

type RouteDir = (String, String);


fn colour_list(count: usize) -> Vec<String> {
    //! Create a vector of hex colour codes, with evenly-spaced hues
    //! plus a little bit of variance in saturation and lightness.
    let mut out = Vec::<String>::new();
    let mut rng = rand::thread_rng();
    for k in 0..count {
        let hue = 360.0 * (k as f64) / (count as f64);
        let sat_var: f64 = rng.gen();
        let sat = 90.0 + 10.0 * sat_var;
        let val = match k % 4 {
            1 => 50.0,
            3 => 60.0,
            _ => 55.0,
        };
        out.push(hsluv_to_hex((hue, sat, val)));
    }
    return out;
}

fn jumbled<T>(input: Vec<T>) -> Vec<T>
where
    T: Clone,
{
    //! If `input.len() >= 2`, returns a copy of `input` with its elements permuted in a star pattern.
    //! Otherwise, returns `input`.

    let L = input.len();

    if L < 2 {
        return input;
    } else if L == 4 {
        return vec![
            input[1].clone(),
            input[3].clone(),
            input[0].clone(),
            input[2].clone(),
        ];
    } else if L == 6 {
        return vec![
            input[1].clone(),
            input[3].clone(),
            input[5].clone(),
            input[0].clone(),
            input[2].clone(),
            input[4].clone(),
        ];
    } else {
        // want g, L to be co-prime for a star pattern
        // if m, n are coprime then more coprime pairs can be generated:
        // (2m - n, m) and (2m + n, m) and (m + 2n, n)
        // always coprime if m = n - 1 (for m >= 3)
        let g = match L % 3 {
            1 => L / 3, // (m + 2n, n) => 3k + 1, k = n
            _ => match L % 9 {
                6 => L / 3 - 1, // 3(3k-1) e.g. 15 so L/3 + 1 is div. by 3
                _ => L / 3 + 1, // (2m + n, m) => 3k + 2, k = n AND ALSO other 3k's
            },
        };

        let mut out = Vec::with_capacity(L);
        for i in 0..L {
            out.push(input[(g * (i + g)) % L].clone());
        }
        return out;
    }
}

fn list_routes(db: &Connection) -> Result<Vec<RouteDir>> {
    //! List all the route/direction combinations
    let mut rdstmt = db.prepare("SELECT DISTINCT route, direction FROM Patronage;")?;

    let mut rd = rdstmt
        .query_map(NO_PARAMS, |row| Ok((row.get_unwrap(0), row.get_unwrap(1))))?
        .filter_map(|l| l.ok())
        .collect::<Vec<RouteDir>>();

    rd.sort_unstable();

    Ok(rd)
}

fn one(db: &Connection, route: &str, direction: &str) -> Result<BTreeMap<(i32, i32), i32>> {
    //! Get a mapping of {(origin, destination) : patronage} for a **single** route/direction pair.

    let mut stmt = db.prepare("SELECT origin_stop, destination_stop, sum(quantity)
        FROM Patronage WHERE route IS :route AND direction IS :direction GROUP BY origin_stop, destination_stop;")
        .expect("Failed preparing statement.");

    let mut tree: BTreeMap<(i32, i32), i32> = BTreeMap::new();

    stmt.query_map_named(&[(":route", &route), (":direction", &direction)], |row| {
        Ok((row.get(0), row.get(1), row.get(2)))
    })?
    .filter_map(|l| l.ok())
    .filter(|l| l.0.is_ok() && l.1.is_ok() && l.2.is_ok())
    .map(|l| (l.0.unwrap(), l.1.unwrap(), l.2.unwrap()))
    .for_each(|r| {
        tree.insert((r.0, r.1), r.2);
    });

    Ok(tree)
}

fn get_boardings(
    db: &Connection,
    route: &str,
    direction: &str,
    stop_id: i64,
) -> Result<i64, rusqlite::Error> {
    //! Get the boardings for one specific stop on a route

    //     println!("{} {} {}", route, direction, stop_id);

    let mut stmt = db.prepare(
        "SELECT SUM(quantity) FROM Patronage 
    WHERE route = :route AND direction = :direction AND origin_stop = :origin_stop;",
    )?;

    stmt.query_row_named(
        &[
            (":route", &route),
            (":direction", &direction),
            (":origin_stop", &stop_id),
        ],
        |row| row.get(0),
    )
}


fn main() {
    let matches = App::new("fluvial")
        .about(crate_description!())
        .version(crate_version!())
        .arg(
            Arg::with_name("infile")
                .takes_value(true)
                .required(true)
                .help("The patronage CSV"),
        )
        .arg(
            Arg::with_name("one")
                .short("o")
                .help("Generate visualisation for only one route/direction combination.")
                .value_names(&["route", "direction"]),
        )
        .arg(
            Arg::with_name("gtfs_dir")
                .short("g")
                .takes_value(true)
                .help("Determine stop names and sequences from a folder of GTFS files"),
        )
        .arg(
            Arg::with_name("list")
                .long("list")
                .help("List all route/direction pairs"),
        )
        .get_matches();

    let db = Connection::open_in_memory().expect("Could not open virtual database");
    rusqlite::vtab::csvtab::load_module(&db)
        .expect("Could not load CSV module of virtual database");

    let mut infilename = matches.value_of("infile").unwrap().replace("'", "''");
    infilename.trim();

    // lack of spaces around = is necessary
    let schema = format!(
        "CREATE VIRTUAL TABLE PInit USING csv(filename='{}', header=YES)",
        infilename
    );

    db.execute_batch(&schema)
        .expect("Error reading the patronage CSV");

    let schema = "CREATE TABLE Patronage(operator TEXT, month TEXT, route TEXT, direction TEXT, time TEXT, ticket_type TEXT, origin_stop INTEGER, destination_stop INTEGER, quantity INTEGER);";

    db.execute_batch(&schema)
        .expect("Failed to create real table.");

    let schema = "INSERT INTO Patronage SELECT * FROM PInit;";

    match db.execute_batch(&schema) {
        Ok(_) => eprintln!("Info: successfully loaded the patronage CSV as a database."),
        Err(e) => eprintln!(
            "Error: read the patronage CSV but could not convert the type affinities: {}",
            e,
        ),
    }

    if matches.is_present("list") {
        match list_routes(&db) {
            Ok(l) => l.iter().for_each(|l| println!("{}\t{}", l.0, l.1)),
            Err(e) => eprintln!("{}", e),
        }
    } else {
        // other info-like options potentially after --list

        // calc/cache GTFS things here
        // some structure of
        if matches.is_present("gtfs_dir") {
            match load_gtfs(&db, PathBuf::from(matches.value_of("gtfs_dir").unwrap())) {
                Ok(_) => eprintln!("Info: successfully loaded GTFS data as a database."),
                Err(e) => {
                    eprintln!("Failed to load GTFS from disk. {:?}", e);
                    exit(1)
                }
            }
        } else {
            eprintln!("Error: Position file or GTFS directory must be specified.");
            std::process::exit(1);
        }

        if matches.is_present("one") {
            let one_rd: Vec<&str> = matches.values_of("one").unwrap().collect();

            let patronages = one(&db, one_rd[0], one_rd[1]).expect("Error collating stop patronage");

            println!("Origin\tDestn.\tQuantity");
            for (k, v) in patronages.iter() {
                println!("{:06}\t{:06}\t{}", k.0, k.1, v);
            }

            let stop_seq: Vec<i64> = match make_stop_sequence(&db, one_rd[0], one_rd[1]) {
                Ok(o) => o,
                Err(e) => {
                    eprintln!(
                        "Error making stop sequences. Does {} {} exist? Perhaps it is seasonal and therefore not in the current GTFS data... try transitfeeds.com to see if they have a historical version.\n{}",
                        one_rd[0], one_rd[1], e
                    );

                    exit(1)
                }
            };

            println!("Stop sequence: {:?}", stop_seq);

        // actual: call SVG gen
        } else {
            //             println!("Route\tDirection\tOrigin\tDestination\tPatronage");
            //             for (route, direction) in list_routes(&db).expect("Failed to list routes").iter() {
            //                 match one(&db, route, direction) {
            //                     Ok(l) => l.iter().for_each(|(k, v)| {
            //                         println!("{}\t{}\t{}\t{}\t{}", route, direction, k.0, k.1, v)
            //                     }),
            //                     Err(e) => eprintln!("{}", e),
            //                 }
            //                 // actual: call SVG gen
            //             }
        }
    }
}

fn convert_direction(from: &str) -> &'static str {
    let froml = from.to_lowercase();
    match froml.as_str() {
        "counterclockwise" => "1",
        "outbound" => "1",
        "south" => "1",
        "west" => "1",
        _ => "0",
    }
}

// SELECT stop_id, stop_sequence FROM StopTimes WHERE trip_id IN (SELECT trip_id FROM Routes, Trips WHERE Routes.route_id IS Trips.route_id AND route_short_name IS 399);
