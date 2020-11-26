//! Fluvial
//!
//! A bit like a Sankey diagram, only a little simpler.
//! Intended for visualising passenger flows over a route.

extern crate hsluv;
#[macro_use]
extern crate clap;

use clap::{App, Arg, ArgGroup};
use hsluv::*;
use rand::Rng;
use rusqlite::{params, Connection, Result, NO_PARAMS};
use std::collections::BTreeMap;

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
            Arg::with_name("list")
                .long("list")
                .help("List all route/direction pairs"),
        )
        .get_matches();

    let db = Connection::open_in_memory().expect("Could not open virtual database.");
    rusqlite::vtab::csvtab::load_module(&db)
        .expect("Could not load CSV module of virtual database.");

    let mut infilename = matches.value_of("infile").unwrap().replace("'", "''");
    infilename.trim();

    eprintln!("{}", infilename);

    // lack of spaces around = is necessary
    let schema = format!(
        "CREATE VIRTUAL TABLE PInit USING csv(filename='{}', header=YES)",
        infilename
    );

    match db.execute_batch(&schema) {
        Ok(_) => eprintln!("Successfully loaded the CSV."),
        Err(err) => eprintln!("Error loading the CSV: {}", err),
    };

    let schema = "CREATE TABLE Patronage(operator TEXT, month TEXT, route TEXT, direction TEXT, time TEXT, ticket_type TEXT, origin_stop INTEGER, destination_stop INTEGER, quantity INTEGER);";

    db.execute_batch(&schema)
        .expect("Failed to create real table.");

    let schema = "INSERT INTO Patronage SELECT * FROM PInit;";

    match db.execute_batch(&schema) {
        Ok(_) => eprintln!("Successfully converted the type affinities."),
        Err(err) => eprintln!("Error converting the type affinities: {}", err),
    };

    if matches.is_present("list") {
        let mut route_dirs: Vec<RouteDir> = Vec::new();

        let mut rdstmt = db
            .prepare("SELECT DISTINCT route, direction FROM Patronage;")
            .expect("Failed preparing statement");

        let rds = rdstmt
            .query_map(NO_PARAMS, |row| Ok((row.get_unwrap(0), row.get_unwrap(1))))
            .expect("Failed to retrieve list of route/direction pairs.")
            .filter_map(|l| l.ok())
            .for_each(|r| {
                println!("{}\t{}", r.0, r.1);
                route_dirs.push(r);
            });
    }

    if matches.is_present("one") {
        let one_rd: Vec<&str> = matches.values_of("one").unwrap().collect();

        let mut stmt = db.prepare("SELECT origin_stop, destination_stop, sum(quantity)
        FROM Patronage WHERE route IS :route AND direction IS :direction GROUP BY origin_stop, destination_stop;")
        .expect("Failed preparing statement.");

        let mut tree: BTreeMap<(i64, i64), i64> = BTreeMap::new();

        stmt.query_map_named(
            &[(":route", &one_rd[0]), (":direction", &one_rd[1])],
            |row| Ok((row.get(0), row.get(1), row.get(2))),
        )
        .expect(&format!(
            "Failed to retrieve patronage info for {} {}",
            &one_rd[0], &one_rd[1]
        ))
        .filter_map(|l| l.ok())
        .filter(|l| l.0.is_ok() && l.1.is_ok() && l.2.is_ok())
        .map(|l| (l.0.unwrap(), l.1.unwrap(), l.2.unwrap()))
        .for_each(|r| {
            //println!("{}\t{}\t{}", r.0, r.1, r.2);
            tree.insert((r.0, r.1), r.2);
        });
    }
}
