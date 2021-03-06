//! Fluvial
//!
//! A bit like a Sankey diagram, only a little simpler.
//! Intended for visualising passenger flows over a route.

extern crate ansi_escapes;
extern crate hsluv;
extern crate structopt;

use rusqlite::{Connection, Result, NO_PARAMS};
use structopt::StructOpt;

use std::collections::BTreeMap;
use std::io::Write;
use std::iter::Iterator;
use std::path::PathBuf;
use std::process::exit;

mod gtfs;
use crate::gtfs::*;

mod visualise;
use crate::visualise::*;

type RouteDir = (String, String);

#[derive(StructOpt)]
#[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
struct Opts {
    /// Colour by destination instead of by origin
    #[structopt(short = "s", long = "swap-colours")]
    swap: bool,
    /// Colour neighbours differently rather than similarly
    #[structopt(short = "j", long = "jumble-colours")]
    jumble: bool,
    /// List all route/direction pairs and exit
    #[structopt(short = "l", long = "list")]
    list: bool,
    /// Print license information and acknowledgements and exit
    #[structopt(short = "L", long = "license")]
    license: bool,
    /// Tell me more
    #[structopt(short = "v", long = "verbose")]
    verbose: bool,
    #[structopt(
    value_names(&["route", "direction"]),
    short = "o", long = "one",
    )]
    /// Generate visualisation for only one route/direction combination
    one: Vec<String>,
    #[structopt(short = "c", long = "css", value_names(&["path"]))]
    /// Path to a custom CSS file for the SVGs
    css: Option<PathBuf>,
    /// Determine stop names and sequences from a folder of GTFS files
    #[structopt(short = "g", long = "gtfs", value_names(&["path"]), required_unless = "pos-file")]
    gtfs_dir: Option<PathBuf>,
    /// Take stop names and sequences from a positions CSV, not GTFS
    #[structopt(short = "p", long = "positions", value_names(&["path"]))]
    pos_file: Option<PathBuf>,
    /// The patronage CSV
    #[structopt(required_unless = "license")]
    in_file: Option<PathBuf>,
    /// Where to put the output SVGs
    out_dir: Option<PathBuf>,
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

fn get_month_year(db: &Connection) -> rusqlite::Result<(String, String)> {
    let mut stmt = db.prepare(
        "SELECT `month`, COUNT(`month`) AS `freq`
    FROM     `Patronage`
    GROUP BY `month`
    ORDER BY `freq` DESC
    LIMIT    1;",
    )?;

    let raw: String = stmt.query_row(NO_PARAMS, |r| r.get(0))?;

    let spl: Vec<&str> = raw.splitn(2, '-').collect();

    Ok((String::from(spl[1]), String::from(spl[0])))
}

fn main() {
    let opts = Opts::from_args();

    if opts.license {
        println!("Fluvial, a transit patronage visualiser.");
        println!("Copyright (c) 2020, Alex Jago.\nhttps://abjago.net\n");
        println!("{}", include_str!("gpl_notice.txt"));
        println!("\nAll components, their authors, and license codes, are listed below:\n");
        println!("{}", include_str!("dependencies.txt"));
        exit(0);
    }

    let now = std::time::Instant::now();

    if !opts.verbose {
        eprintln!("Loading databases...");
    }

    let db = Connection::open_in_memory().expect("Could not open virtual database");
    rusqlite::vtab::csvtab::load_module(&db)
        .expect("Could not load CSV module of virtual database");

    let infilename = opts.in_file.expect("Patronage CSV not supplied");

    // lack of spaces around = is necessary
    let schema = format!(
        "CREATE VIRTUAL TABLE PInit USING csv(filename='{}', header=YES)",
        infilename.display()
    );

    db.execute_batch(&schema)
        .expect("Error reading the patronage CSV");

    let schema = "CREATE TABLE Patronage(operator TEXT, month TEXT, route TEXT, direction TEXT, time TEXT, ticket_type TEXT, origin_stop INTEGER, destination_stop INTEGER, quantity INTEGER);";

    db.execute_batch(&schema)
        .expect("Failed to create real table.");

    let schema = "INSERT INTO Patronage SELECT * FROM PInit;";

    match db.execute_batch(&schema) {
        Ok(_) => {
            if opts.verbose {
                eprintln!("Info: successfully loaded the patronage CSV as a database.")
            }
        }
        Err(e) => eprintln!(
            "Error: read the patronage CSV but could not convert the type affinities: {}",
            e,
        ),
    }

    if opts.list {
        match list_routes(&db) {
            Ok(l) => l.iter().for_each(|l| println!("{}\t{}", l.0, l.1)),
            Err(e) => eprintln!("{}", e),
        }
    } else {
        // other info-like options potentially after --list

        if opts.pos_file.is_some() {
            eprintln!("Sorry, positions files aren't supported yet.");
            exit(1);
        }

        // calc/cache GTFS things here
        if opts.verbose && opts.gtfs_dir.is_some() {
            eprintln!("Loading GTFS. This may take several seconds...");
        }

        match load_gtfs(
            &db,
            PathBuf::from(
                opts.gtfs_dir.unwrap(), /*matches.value_of("gtfs_dir").unwrap()*/
            ),
        ) {
            Ok(_) => {
                if opts.verbose {
                    eprintln!(
                        "Info: successfully loaded GTFS data as a database in {} seconds.",
                        now.elapsed().as_secs()
                    )
                }
            }
            Err(e) => {
                eprintln!("Failed to load GTFS from disk. {:?}", e);
                exit(1)
            }
        }

        let outdir = match opts.out_dir {
            Some(o) => o,
            None => std::env::current_dir().unwrap(),
        };

        let (month, year) = get_month_year(&db).unwrap();
        let mut rds: Vec<RouteDir> = Vec::with_capacity(1);

        // {route : [directions]}
        let mut rdt: BTreeMap<String, Vec<String>> = BTreeMap::new();

        if opts.one.len() == 2 {
            rds.push((
                String::from(opts.one[0].clone()),
                String::from(opts.one[1].clone()),
            ));
        } else {
            rds = list_routes(&db).expect("Failed to list routes");
        }

        //eprintln!("rds: {:?}", rds);
        let mut done = 0_usize;
        let mut skipped = 0_usize;
        let total = rds.len();


        if !opts.verbose {
            eprint!("{}", ansi_escapes::CursorPrevLine)
        }

        eprintln!(
            "0 routes done; 0 skipped (not in GTFS); {} total in patronage CSV",
            total
        );

        for (route, direction) in rds {
            if opts.verbose {
                eprintln!("{} {}", route, direction);
            }

            let patronages = one(&db, &route, &direction).expect("Error collating stop patronage");

            let stop_seq: Vec<i64> = match make_stop_sequence(&db, &route, &direction) {
                Ok(o) => o,
                Err(e) => {
                    if opts.one.len() == 2 {
                        eprintln!(
                            "Error making stop sequences. Does {} {} exist? Perhaps it is seasonal and therefore not in the current GTFS data... try transitfeeds.com to see if they have a historical version.\n{}",
                            route, direction, e
                        );
                        exit(1)
                    } else {
                        if opts.verbose {
                            eprintln!(
                                "{} {} {} not in GTFS; skipping",
                                ansi_escapes::CursorPrevLine,
                                route,
                                direction
                            );
                        }
                        skipped = skipped + 1;
                        continue;
                    }
                }
            };

            let stop_names = get_stop_names(&db, &stop_seq).unwrap();

            let service_count = get_service_count(&db, &route, &direction, &month, &year).unwrap();

            let out = visualise_one(
                patronages,
                stop_seq,
                stop_names,
                service_count,
                &route,
                &direction,
                convert_monthname(&month),
                &year,
                opts.swap,
                opts.jumble,
                &opts.css,
            )
            .expect("Error generating SVG");

            let mut outfile = PathBuf::from(&outdir);
            outfile.push(&year);
            outfile.push(&month);
            // eprintln!("Creating {}", outfile.display());
            std::fs::create_dir_all(&outfile).expect("Error creating directory structure");
            outfile.push(format!("{}_{}.svg", route, direction));
            // eprintln!("Writing to {}", outfile.display());
            std::fs::write(outfile, out).expect("Error writing file");

            // do this right at the end, so that if anything else causes a skip,
            // it won't be in the index
            if !rdt.contains_key(&route) {
                rdt.insert(route.clone(), vec![direction.clone()]);
            } else {
                rdt.get_mut(&route).unwrap().push(direction.clone());
            }

            done = done + 1;

            if !opts.verbose {
                eprintln!(
                    "{}{} routes done; {} skipped (not in GTFS); {} total in patronage CSV",
                    ansi_escapes::CursorPrevLine,
                    done,
                    skipped,
                    total
                );
            }
        }

        // Write index.html if not a --one
        if opts.one.len() != 2 {

            let mut index_html = format!(r#"<html>
<head>
<body>
<h4 style="margin-left: 1vw">{} {}</h4>
<table>"#, convert_monthname(&month), &year);

            for (k, v) in rdt {
                index_html.push_str("<tr>");
                for d in v {
                    index_html.push_str(&format!(r#"<td><a href="{}_{}.svg">{} {}</a></td>"#, k, d, k, d));
                }
                index_html.push_str("</tr>")
            }
            index_html.push_str("</table>\n</body>\n</html>");

            let mut outfile = PathBuf::from(&outdir);
            outfile.push(&year);
            outfile.push(&month);
            std::fs::create_dir_all(&outfile).expect("Error creating directory structure");
            outfile.push("index.html");
            std::fs::write(outfile, index_html).expect("Error writing file");
        }

        if opts.verbose {
            eprintln!(
                "Finished everything in {} seconds!",
                now.elapsed().as_secs()
            )
        } else {
            eprintln!(
                "{}{} routes done; {} skipped (not in GTFS); {} total in patronage CSV; completed in {} seconds.",
                ansi_escapes::CursorPrevLine,
                done,
                skipped,
                total,
                now.elapsed().as_secs()
            );
        }

    }
}

fn convert_direction(from: &str) -> &'static str {
    //! Convert a direction name like "inbound" to a "0" or "1"  
    let froml = from.to_lowercase();
    match froml.as_str() {
        "counterclockwise" => "1",
        "outbound" => "1",
        "south" => "1",
        "west" => "1",
        _ => "0",
    }
}

fn convert_monthname(from: &str) -> &str {
    let m: u8 = from.parse().unwrap();
    match m {
        1 => "January",
        2 => "February",
        3 => "March",
        4 => "April",
        5 => "May",
        6 => "June",
        7 => "July",
        8 => "August",
        9 => "September",
        10 => "October",
        11 => "November",
        12 => "December",
        _ => from,
    }
}

fn days_per_month(month: &str, year: &str) -> f32 {
    //! Returns the number of days per month (e.g. January = 31)
    //! month and year should be digits, not names... (and January = 1)
    let m: u8 = month.parse().unwrap();
    let y: usize = year.parse().unwrap();
    let leap: bool = (y % 4 == 0) && ((y % 100 != 0) || (y % 400 == 0));

    match m {
        9 | 4 | 6 | 11 => 30.0, // (1) 30 days hath September, April, June and November,
        2 => match leap {
            // (3) except February,
            true => 29.0,  // (3b) and 29 days each leap year.
            false => 28.0, // (3a) which has 28 days clear,
        },
        _ => 31.0, // (2) all the rest have 31,
    }
}