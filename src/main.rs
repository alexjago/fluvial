//! Fluvial
//!
//! A bit like a Sankey diagram, only a little simpler.
//! Intended for visualising passenger flows over a route.

extern crate ansi_escapes;
extern crate hsluv;
extern crate structopt;

use rusqlite::{named_params, Connection, Result};
use structopt::StructOpt;
use tempfile::{NamedTempFile, TempDir};

use std::collections::BTreeMap;
use std::io::{Cursor, Write};
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use std::process::exit;

mod gtfs;
use crate::gtfs::*;

mod visualise;
use crate::visualise::*;
use std::fs::File;

type RouteDir = (String, String);

#[derive(Debug, StructOpt)]
#[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
#[structopt(about)]
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
    /// Get all utility scripts at https://github.com/alexjago/fluvial/tree/master/utils
    #[structopt(short = "U", long = "utilities")]
    utilities: bool,
    #[structopt(long = "ftime")]
    /// Filter patronage by time of day (by matching in the `time` column)
    ftime: Option<String>,
    #[structopt(
    value_names(&["route", "direction"]),
    short = "o", long = "one",
    )]
    /// Generate visualisation for only one route/direction combination
    one: Vec<String>,
    #[structopt(short = "c", long = "css", value_names(&["path"]))]
    /// Path to a custom CSS file for the SVGs
    css: Option<PathBuf>,
    #[structopt(short = "b", long = "batch", conflicts_with = "license")]
    /// Treat in_file as a batch CSV of <patronage zip URL>, <gtfs zip URL>; conflicts with --gtfs
    batch: bool,
    /// A directory/URI of GTFS files to determine stop names and sequences from
    #[structopt(short = "g", long = "gtfs", value_names(&["path"]), required_unless_one = &["batch", "license", "utilities"], conflicts_with = "batch")]
    gtfs_dir: Option<PathBuf>,
    /// The path/URI of the patronage CSV (or path to batch file, with --batch)
    #[structopt(required_unless_one = &["license", "utilities"])]
    in_file: Option<PathBuf>,
    /// Where to put the output SVGs
    out_dir: Option<PathBuf>,
}

fn list_routes(db: &Connection) -> Result<Vec<RouteDir>> {
    //! List all the route/direction combinations
    let mut rdstmt = db.prepare("SELECT DISTINCT route, direction FROM Patronage;")?;

    let mut rd = rdstmt
        .query_map([], |row| Ok((row.get_unwrap(0), row.get_unwrap(1))))?
        .filter_map(|l| l.ok())
        .collect::<Vec<RouteDir>>();

    rd.sort_unstable();

    Ok(rd)
}
#[inline(never)]
fn make_one(
    db: &Connection,
    route: &str,
    direction: &str,
    ftime: &Option<String>,
) -> Result<BTreeMap<(i32, i32), i32>> {
    //! Get a mapping of {(origin, destination) : patronage} for a **single** route/direction pair.

    // This function is responsible for half the CPU-time as of v0.3.3. Tread carefully.
    // The #[inline(never)] is removable when not profiling
    // According to the flamegraph almost all the time is spent down in SQLite proper
    // So the Rust-side code is probably fine.

    // The time filtering is a bit wacky. Something like `time IS *` in a WHERE clause isn't allowed
    // but it *is* OK to do `((time IS ...) OR (1=1))`. The rest of the madness is just to
    // ensure that we always pass one parameter for :time and to handle --ftime NULL

    let ftime_ins = match ftime {
        Some(_) => String::from("AND (time IS :time)"),
        None => String::from("AND ((time IS :time) OR (1=1))"),
    };

    let ftime_sub = match ftime {
        Some(s) => s.clone(),
        None => String::from("NULL"),
    };

    let stmt_txt = format!("SELECT origin_stop, destination_stop, sum(quantity)
        FROM Patronage WHERE route IS :route AND direction IS :direction {} GROUP BY origin_stop, destination_stop;", ftime_ins);

    let mut stmt = db.prepare(&stmt_txt).expect("Failed preparing statement.");

    let mut tree: BTreeMap<(i32, i32), i32> = BTreeMap::new();

    stmt.query_map(
        named_params! {
            ":route": &route,
            ":direction": &direction,
            ":time": &ftime_sub.as_str(),
        },
        |row| Ok((row.get(0), row.get(1), row.get(2))),
    )?
    .filter_map(|l| l.ok())
    .filter(|l| l.0.is_ok() && l.1.is_ok() && l.2.is_ok())
    .map(|l| (l.0.unwrap(), l.1.unwrap(), l.2.unwrap()))
    .for_each(|r| {
        tree.insert((r.0, r.1), r.2);
    });

    Ok(tree)
}

#[inline(never)]
fn get_boardings(
    // used in gtfs.rs for stop sequencing
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

    stmt.query_row(
        named_params! {
            ":route": &route,
            ":direction": &direction,
            ":origin_stop": &stop_id,
        },
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

    let raw: String = stmt.query_row([], |r| r.get(0))?;

    let spl: Vec<&str> = raw.splitn(2, '-').collect();

    Ok((String::from(spl[1]), String::from(spl[0])))
}

fn main() {
    let opts = Opts::from_args();

    if opts.verbose {
        eprintln!("{:#?}", opts);
    }

    if opts.license {
        println!("Fluvial, a transit patronage visualiser.");
        println!("Copyright (c) 2020, Alex Jago.\nhttps://abjago.net\n");
        println!("{}", include_str!("gpl_notice.txt"));
        println!("\nAll components, their authors, source code repositories, and license details are listed below:\n");
        println!("{}", include_str!("dependencies.txt"));
        /* Before releasing a new version, run...
           cargo-license --avoid-build-deps --avoid-dev-deps -a -t > src/dependencies.txt
        */
        exit(0);
    }

    if opts.utilities {
        println!("Get all utility scripts for Fluvial at\nhttps://github.com/alexjago/fluvial/tree/master/utils");
        exit(0);
    }

    if opts.batch {
        // TODO:
        // if path is "-" then it's std input
        // thanks /u/burntsushi
        // https://www.reddit.com/r/rust/comments/jv3q3e/how_to_select_between_reading_from_a_file_and/gci1mww/
        let path = opts
            .in_file
            .expect("Please specify a file path '-' for standard input");

        let batch_stream: Box<dyn std::io::Read + 'static> = if path.as_os_str() == "-" {
            Box::new(std::io::stdin())
        } else {
            Box::new(std::fs::File::open(&path).expect("Error opening batch file for reading"))
        };

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(batch_stream);

        for r in rdr.records().filter_map(|s| s.ok()) {
            let patronage_uri = PathBuf::from(r.get(0).expect("Missing patronage URI!"));
            let gtfs_uri = PathBuf::from(r.get(1).expect("Missing GTFS URI!"));

            single_month(
                &opts.verbose,
                &Some(patronage_uri),
                &opts.list,
                &Some(gtfs_uri),
                &opts.out_dir,
                &opts.one,
                &opts.ftime,
                &opts.swap,
                &opts.jumble,
                &opts.css,
            );
        }
    } else {
        // no downloads or anything, just go
        single_month(
            &opts.verbose,
            &opts.in_file,
            &opts.list,
            &opts.gtfs_dir,
            &opts.out_dir,
            &opts.one,
            &opts.ftime,
            &opts.swap,
            &opts.jumble,
            &opts.css,
        );
    }
}

fn single_month(
    verbose: &bool,
    in_file: &Option<PathBuf>,
    list: &bool,
    gtfs_dir: &Option<PathBuf>,
    out_dir: &Option<PathBuf>,
    one: &[String],
    ftime: &Option<String>,
    swap: &bool,
    jumble: &bool,
    css: &Option<PathBuf>,
) {
    //! Run a single month's worth of processing.
    let now = std::time::Instant::now();

    if *verbose {
        eprintln!("Loading databases...");
    }

    let db = Connection::open_in_memory().expect("Could not open virtual database");
    rusqlite::vtab::csvtab::load_module(&db)
        .expect("Could not load CSV module of virtual database");

    let mut infilename = in_file
        .as_ref()
        .expect("Patronage CSV not supplied")
        .clone();

    // now for the big mess
    let mut pat_tmpfile: Option<NamedTempFile> = None; // up here for scope reasons

    if !Path::new(&infilename).exists() {
        // attempt to download it to a temporary file instead
        if *verbose {
            eprintln!("Downloading {:#?}", infilename);
        }
        pat_tmpfile = Some(NamedTempFile::new().expect("Error creating temporary file"));
        let resp = reqwest::blocking::get(infilename.to_str().unwrap())
            .expect("Could not find or download patronage data");

        if *verbose {
            eprintln!("{:#?}", resp.headers());
        }

        let bytes = resp.bytes().unwrap();

        if tree_magic_mini::match_u8("application/zip", &bytes) {
            // this is a zipfile lol
            let cur = Cursor::new(bytes);
            // Reqwest responses are merely Read, not Seek.. but Cursors are, so maybe this will work
            let mut zippy = zip::ZipArchive::new(cur).expect("Error reading downloaded ZIP");
            for i in 0..zippy.len() {
                let mut f = zippy.by_index(i).expect("Error unzipping");
                if f.name().ends_with(".csv") {
                    eprintln!("Extracting: {}", f.name());
                    std::io::copy(&mut f, &mut pat_tmpfile.as_ref().unwrap())
                        .expect("Error storing Patronage CSV");
                    break;
                }
            }
        } else if tree_magic_mini::match_u8("text/csv", &bytes) {
            // copy and hope
            pat_tmpfile
                .as_ref()
                .unwrap()
                .write_all(&bytes)
                .expect("Error saving patronage data");
        }
        infilename = pat_tmpfile.as_ref().unwrap().path().to_path_buf();
    }

    let pre_patronage_load_time = std::time::Instant::now();

    // lack of spaces around = is necessary
    let schema = format!(
        "CREATE VIRTUAL TABLE PInit USING csv(filename='{}', header=YES)",
        infilename.display()
    );

    match db.execute_batch(&schema) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Error reading the patronage CSV:");
            eprintln!("{}", e);
            eprintln!("Skipping this month.");
            if (&pat_tmpfile).is_some() {
                let _mv = pat_tmpfile
                    .unwrap()
                    .persist("./error.csv")
                    .expect("Error persisting CSV for inspection");
                eprintln!("Inspect the erroneous CSV at\n./error.csv");
            }
            return;
        }
    }

    let schema = "CREATE TABLE Patronage(operator TEXT, month TEXT, route TEXT, direction TEXT, time TEXT, ticket_type TEXT, origin_stop INTEGER, destination_stop INTEGER, quantity INTEGER);";

    db.execute_batch(schema)
        .expect("Failed to create real table.");

    let schema = "INSERT INTO Patronage SELECT * FROM PInit;";

    match db.execute_batch(schema) {
        Ok(_) => {
            if *verbose {
                eprintln!(
                    "Info: successfully loaded the patronage CSV as a database in {} seconds.",
                    pre_patronage_load_time.elapsed().as_secs()
                )
            }
        }
        Err(e) => {
            eprintln!(
                "Error: read the patronage CSV but could not convert the type affinities: {}",
                e,
            );
            eprintln!("Skipping this month.");
            return;
        }
    }

    // Prep an index. This alone practically halved the runtime when added.
    match db.execute_batch("CREATE INDEX idx_patronage_routedir on Patronage(route, direction);") {
        Err(e) => eprintln!(
            "Warning: error creating index on patronage database; performance may be degraded\n{}",
            e
        ),
        _ => (),
    }

    if *verbose {
        eprintln!(
            "Loaded patronage CSV in {}s (possibly after downloading)",
            pre_patronage_load_time.elapsed().as_secs()
        )
    }

    if *list {
        match list_routes(&db) {
            Ok(l) => l.iter().for_each(|l| println!("{}\t{}", l.0, l.1)),
            Err(e) => eprintln!("{}", e),
        }
    } else {
        // other info-like options potentially after --list

        // calc/cache GTFS things here
        if *verbose && gtfs_dir.is_some() {
            eprintln!("Loading GTFS. This may take several seconds...");
        }

        // Attempt download if not a local path
        let mut gtfs_actual_dir = gtfs_dir.as_ref().unwrap().clone();
        let gtfs_tmpdir: TempDir; // needed for scope
        if !Path::new(gtfs_dir.as_ref().unwrap()).exists() {
            // gotta download the thing
            if *verbose {
                eprintln!("Downloading {:#?}", gtfs_dir.as_ref().unwrap());
            }
            gtfs_tmpdir = tempfile::tempdir().expect("Could not create temporary GTFS directory");
            let mut tmp_path = gtfs_tmpdir.path().to_path_buf();

            let resp = reqwest::blocking::get(gtfs_dir.as_ref().unwrap().to_str().unwrap())
                .expect("Could not find or download patronage data");

            if *verbose {
                eprintln!("{:#?}", resp.headers());
            }

            let bytes = resp.bytes().unwrap();

            if tree_magic_mini::match_u8("application/zip", &bytes) {
                let cur = Cursor::new(bytes);
                let mut zippy = zip::ZipArchive::new(cur).expect("Error reading downloaded ZIP");

                for i in 0..zippy.len() {
                    let mut zfile = zippy.by_index(i).expect("Error reading GTFS ZIP");
                    tmp_path.push(zfile.name());
                    let mut actual = File::create(&tmp_path).expect("Error saving GTFS file");
                    std::io::copy(&mut zfile, &mut actual).expect("Error extracting from GTFS ZIP");
                    tmp_path.pop();
                }
            } else {
                eprintln!("Didn't download a (GTFS) zipfile. Skipping this month.");
                return;
            }
            gtfs_actual_dir = tmp_path;
        }

        match load_gtfs(&db, gtfs_actual_dir) {
            Ok(_) => {
                if *verbose {
                    eprintln!(
                        "Info: successfully loaded GTFS data as a database in {} seconds.",
                        now.elapsed().as_secs()
                    )
                }
            }
            Err(e) => {
                eprintln!(
                    "Failed to load GTFS from disk; skipping this month. {:?}",
                    e
                );
                return;
            }
        }

        // Output Directory
        let outdir = match out_dir {
            Some(o) => o.clone(),
            None => std::env::current_dir().unwrap(),
        };

        // Month and Year
        let (month, year) = get_month_year(&db).unwrap();
        let mut rds: Vec<RouteDir> = Vec::with_capacity(1);

        // {route : [directions]}
        let mut rdt: BTreeMap<String, Vec<String>> = BTreeMap::new();

        if one.len() == 2 {
            rds.push((one[0].clone(), one[1].clone()));
        } else {
            rds = list_routes(&db).expect("Failed to list routes");
        }

        //eprintln!("rds: {:?}", rds);
        let mut done = 0_usize;
        let mut skipped = 0_usize;
        let total = rds.len();

        if !verbose {
            eprint!("{}", ansi_escapes::CursorPrevLine)
        }

        eprintln!(
            "0 routes done; 0 skipped (not in GTFS); {} total in patronage CSV",
            total
        );

        for (route, direction) in rds {
            if *verbose {
                eprintln!("{} {}", route, direction);
            }

            let patronages =
                make_one(&db, &route, &direction, ftime).expect("Error collating stop patronage");

            let stop_seq: Vec<i64> = match make_stop_sequence(&db, &route, &direction) {
                Ok(o) => o,
                Err(e) => {
                    if one.len() == 2 {
                        eprintln!(
                            "Error making stop sequences. Does {} {} exist? Perhaps it is seasonal and therefore not in the current GTFS data... try transitfeeds.com to see if they have a historical version.\n{}",
                            route, direction, e
                        );
                        exit(1)
                    } else {
                        if *verbose {
                            eprintln!(
                                "{} {} {} not in GTFS; skipping",
                                ansi_escapes::CursorPrevLine,
                                route,
                                direction
                            );
                        }
                        skipped += 1;
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
                ftime,
                convert_monthname(&month),
                &year,
                *swap,
                *jumble,
                css,
            )
            .expect("Error generating SVG");

            write_outfile(
                &outdir,
                &format!("{}_{}.svg", route, direction),
                &month,
                &year,
                ftime,
                &out,
            )
            .expect("Error writing SVG file");

            // do this right at the end, so that if anything else causes a skip,
            // it won't be in the index
            if !rdt.contains_key(&route) {
                rdt.insert(route.clone(), vec![direction.clone()]);
            } else {
                rdt.get_mut(&route).unwrap().push(direction.clone());
            }

            done += 1;

            if !verbose {
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
        if one.len() != 2 {
            let mut index_html = format!(
                r#"<html>
<head>
<body>
<h4 style="margin-left: 1vw">{} {}</h4>
<table>
"#,
                convert_monthname(&month),
                &year
            );

            for (k, v) in rdt {
                index_html.push_str("<tr>");
                for d in v {
                    index_html.push_str(&format!(
                        r#"<td><a href="{}_{}.svg">{} {}</a></td>"#,
                        k, d, k, d
                    ));
                }
                index_html.push_str("</tr>\n")
            }
            index_html.push_str("</table>\n</body>\n</html>");

            write_outfile(&outdir, "index.html", &month, &year, ftime, &index_html)
                .expect("Error writing index");
        }

        if *verbose {
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

fn write_outfile(
    out_dir: &Path,
    filename: &str,
    month: &str,
    year: &str,
    ftime: &Option<String>,
    contents: &str,
) -> std::result::Result<(), std::io::Error> {
    let mut outfile = PathBuf::from(&out_dir);
    outfile.push(&year);
    outfile.push(&month);
    if ftime.is_some() {
        outfile.push(
            ftime
                .as_ref()
                .unwrap()
                .replace(' ', "_")
                .replace('(', "")
                .replace(')', "")
                .replace(':', ""),
        );
    }
    std::fs::create_dir_all(&outfile)?;
    outfile.push(filename);
    std::fs::write(outfile, contents)?;
    Ok(())
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
