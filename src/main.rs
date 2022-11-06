//! Fluvial
//!
//! A bit like a Sankey diagram, only a little simpler.
//! Intended for visualising passenger flows over a route.

// TODO: refactor with proper errors / logging`

// LINTS
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::style)]
// The spicy one
#![warn(clippy::restriction)]
// restriction allows
#![allow(clippy::implicit_return)]
#![allow(clippy::float_arithmetic)]
#![allow(clippy::integer_arithmetic)]
#![allow(clippy::integer_division)]
#![allow(clippy::indexing_slicing)]
#![allow(clippy::default_numeric_fallback)]
// restriction warns
#![warn(clippy::unwrap_used)]
#![warn(clippy::expect_used)]
// pedantic allows
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::cast_precision_loss)]

extern crate ansi_escapes;
extern crate hsluv;
extern crate structopt;

use anyhow::{anyhow, bail, Context, Result};
use gtfs::Quantity;
use rusqlite::{named_params, Connection};
use structopt::StructOpt;
use tempfile::{NamedTempFile, TempDir};
use ureq::Agent;

use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;
use std::io::{Cursor, Read, Write};
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use std::process::exit;

mod gtfs;
use crate::gtfs::{get_service_count, get_stop_names, load_gtfs, make_stop_sequence, StopId};

mod visualise;
use crate::visualise::visualise_one;
use std::fs::File;

/// A (route, direction) pair
type RouteDir = (String, String);

#[allow(clippy::struct_excessive_bools)]
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
    // #[structopt(required_unless_one = &["license", "utilities"])]
    in_file: Option<PathBuf>,
    /// Where to put the output SVGs
    out_dir: Option<PathBuf>,
}

fn list_routes(db: &Connection) -> Result<Vec<RouteDir>> {
    //! List all the route/direction combinations
    let mut rdstmt = db.prepare("SELECT DISTINCT route, direction FROM Patronage;")?;

    let mut rd = rdstmt
        .query_map([], |row| Ok((row.get_unwrap(0), row.get_unwrap(1))))?
        .filter_map(std::result::Result::ok)
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
) -> Result<BTreeMap<(StopId, StopId), Quantity>> {
    //! Get a mapping of {(origin, destination) : patronage} for a **single** route/direction pair.

    // The time filtering is a bit wacky. Something like `time IS *` in a WHERE clause isn't allowed
    // but it *is* OK to do `((time IS ...) OR (1=1))`. The rest of the madness is just to
    // ensure that we always pass one parameter for :time and to handle --ftime NULL

    let ftime_ins = match *ftime {
        Some(_) => String::from("AND (time IS :time)"),
        None => String::from("AND ((time IS :time) OR (1=1))"),
    };

    let ftime_sub = match ftime.as_ref() {
        Some(s) => s.clone(),
        None => String::from("NULL"),
    };

    let stmt_txt = format!("SELECT origin_stop, destination_stop, sum(quantity)
        FROM Patronage WHERE route IS :route AND direction IS :direction {} GROUP BY origin_stop, destination_stop;", ftime_ins);

    let mut stmt = db.prepare(&stmt_txt).context("Failed preparing statement.")?;

    let mut tree = BTreeMap::new();

    stmt.query_map(
        named_params! {
            ":route": &route,
            ":direction": &direction,
            ":time": &ftime_sub.as_str(),
        },
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?
    .filter_map(core::result::Result::ok)
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
    stop_id: u32,
) -> Result<u32, rusqlite::Error> {
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

fn main() -> Result<()> {
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
        let path = opts.in_file.context("Please specify a file path '-' for standard input")?;

        let batch_stream: Box<dyn std::io::Read + 'static> = if path.as_os_str() == "-" {
            Box::new(std::io::stdin())
        } else {
            Box::new(std::fs::File::open(&path).context("Error opening batch file for reading")?)
        };

        let mut rdr = csv::ReaderBuilder::new().has_headers(false).from_reader(batch_stream);

        for r in rdr.records().filter_map(std::result::Result::ok) {
            let patronage_uri = PathBuf::from(r.get(0).context("Missing patronage URI!")?);
            let gtfs_uri = PathBuf::from(r.get(1).context("Missing GTFS URI!")?);

            if let Err(e) = single_month(
                opts.verbose,
                &Some(patronage_uri),
                opts.list,
                &Some(gtfs_uri),
                &opts.out_dir,
                &opts.one,
                &opts.ftime,
                opts.swap,
                opts.jumble,
                &opts.css,
            ) {
                eprintln!("Skipping this month: {e}");
            }
        }
    } else {
        // no downloads or anything, just go
        single_month(
            opts.verbose,
            &opts.in_file,
            opts.list,
            &opts.gtfs_dir,
            &opts.out_dir,
            &opts.one,
            &opts.ftime,
            opts.swap,
            opts.jumble,
            &opts.css,
        )?;
    }
    Ok(())
}

#[allow(clippy::fn_params_excessive_bools)]
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
fn single_month(
    verbose: bool,
    in_file: &Option<PathBuf>,
    list: bool,
    gtfs_dir: &Option<PathBuf>,
    out_dir: &Option<PathBuf>,
    one: &[String],
    ftime: &Option<String>,
    swap: bool,
    jumble: bool,
    css: &Option<PathBuf>,
) -> Result<()> {
    //! Run a single month's worth of processing.
    let now = std::time::Instant::now();

    if verbose {
        eprintln!("Loading databases...");
    }

    let db = Connection::open_in_memory().context("Could not open virtual database")?;
    rusqlite::vtab::csvtab::load_module(&db)
        .context("Could not load CSV module of virtual database")?;
    // Sign up for 512 MB of mmap if we can
    if let Err(e) = db.pragma_update(None, "mmap_size", 1 << 29) {
        eprintln!("Warn: mmap unsuccessful; performance may be degraded.\n{e}",);
    }

    let dl_agent = ureq::AgentBuilder::new()
        .user_agent("fluvial/0.3.4")
        .tls_connector(std::sync::Arc::new(native_tls::TlsConnector::new()?))
        .build();

    let pre_patronage_load_time = std::time::Instant::now();

    let pat_tmpfile = {
        if let Some(x) = in_file.as_ref().filter(|p| !p.exists()) {
            // Patronage CSV doesn't exist on disk, so let's try to download it
            Some(download_patronage(&dl_agent, x, verbose)?)
        } else {
            // Patronage CSV exists on disk, no need to download it
            None
        }
    };

    let infilename: PathBuf = PathBuf::from(match pat_tmpfile.as_ref() {
        Some(x) => x.path(),
        None => in_file.as_ref().context("Missing patronage CSV")?.as_path(),
    });

    load_patronage(&db, &infilename, pat_tmpfile)?;

    if verbose {
        eprintln!(
            "Loaded patronage CSV in {}s (possibly after downloading)",
            pre_patronage_load_time.elapsed().as_secs()
        );
    }

    if list {
        match list_routes(&db) {
            Ok(l) => {
                for k in &l {
                    println!("{}\t{}", k.0, k.1);
                }
                Ok(())
            }
            Err(e) => bail!(e),
        }
    } else {
        // other info-like options potentially after --list

        // Download GTFS if it doesn't exist
        if verbose && gtfs_dir.is_some() {
            eprintln!("Loading GTFS. This may take several seconds...");
        }

        // for lifetime reasons, we get a tempdir this way...
        let gtfs_tempdir: Option<TempDir> = {
            if let Some(x) = gtfs_dir.as_ref().filter(|x| !(x.exists())) {
                Some(
                    download_gtfs(&dl_agent, x, verbose)
                        .context("Didn't download a (GTFS) zip file. Skipping this month.")?,
                )
            } else {
                None
            }
        };

        let gtfs_actual_dir = match gtfs_tempdir.as_ref() {
            Some(x) => x.path(),
            None => gtfs_dir.as_ref().context("Missing GTFS directory")?.as_path(),
        };

        match load_gtfs(&db, gtfs_actual_dir) {
            Ok(_) => {
                if verbose {
                    eprintln!(
                        "Info: successfully loaded GTFS data as a database in {} seconds.\n\n",
                        now.elapsed().as_millis() as f32 / 1000.0
                    );
                }
            }
            Err(e) => {
                bail!("Failed to load GTFS from disk; skipping this month. {:?}", e);
            }
        }

        // Output Directory
        let out_dir = match out_dir.as_ref() {
            Some(o) => o.clone(),
            None => std::env::current_dir()?,
        };

        // Month and Year
        let (month, year) = get_month_year(&db)?;
        let mut rd_seq: Vec<RouteDir> = Vec::with_capacity(1);

        // {route : [directions]}
        let mut rd_tree: BTreeMap<String, Vec<String>> = BTreeMap::new();

        if one.len() == 2 {
            rd_seq.push((one[0].clone(), one[1].clone()));
        } else {
            rd_seq = list_routes(&db).context("Failed to list routes")?;
        }

        //eprintln!("rds: {:?}", rds);
        let mut completed = 0_usize;
        let mut skipped = 0_usize;
        let total = rd_seq.len();

        // if !verbose {
        //     eprint!("{}", ansi_escapes::CursorPrevLine)
        // }

        eprintln!("0 routes done; 0 skipped (not in GTFS); {} total in patronage CSV", total);

        for (route, direction) in rd_seq {
            if verbose {
                eprintln!("{} {}", route, direction);
            }

            let patronages = make_one(&db, &route, &direction, ftime)
                .context("Error collating stop patronage")?;

            let stop_seq: Vec<StopId> = match make_stop_sequence(&db, &route, &direction) {
                Ok(o) => o,
                Err(e) => {
                    if one.len() == 2 {
                        bail!(
                            "Error making stop sequences. Does {} {} exist? Perhaps it is seasonal and therefore not in the current GTFS data... try transitfeeds.com to see if they have a historical version.\n{}",
                            route, direction, e
                        );
                    }
                    if verbose {
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
            };

            let stop_names = get_stop_names(&db, &stop_seq)?;

            let service_count = get_service_count(&db, &route, &direction, &month, &year)?;

            let out = visualise_one(
                &patronages,
                &stop_seq,
                &stop_names,
                service_count,
                &route,
                &direction,
                ftime,
                convert_monthname(&month),
                &year,
                swap,
                jumble,
                css,
            )
            .context("Error generating SVG")?;

            write_outfile(
                &out_dir,
                &format!("{}_{}.svg", route, direction),
                &month,
                &year,
                ftime,
                &out,
            )
            .context("Error writing SVG file")?;

            // do this right at the end, so that if anything else causes a skip,
            // it won't be in the index
            rd_tree.entry(route.clone()).or_insert_with(Vec::new).push(direction.clone());

            completed += 1;

            if !verbose {
                eprintln!(
                    "{}{} routes done; {} skipped (not in GTFS); {} total in patronage CSV",
                    ansi_escapes::CursorPrevLine,
                    completed,
                    skipped,
                    total
                );
            }
        }

        // Write index.html if not a --one
        if one.len() != 2 {
            write_index_html(&rd_tree, &out_dir, &month, &year, ftime)?;
        }

        if verbose {
            eprintln!(
                "Finished everything in {} seconds!",
                now.elapsed().as_millis() as f32 / 1000.0
            );
        } else {
            eprintln!(
                "{}{} routes done; {} skipped (not in GTFS); {} total in patronage CSV; completed in {} seconds.",
                ansi_escapes::CursorPrevLine,
                completed,
                skipped,
                total,
                now.elapsed().as_millis() as f32 / 1000.0
            );
        }
        Ok(())
    }
}

fn download_patronage(dl_agent: &Agent, in_file: &PathBuf, verbose: bool) -> Result<NamedTempFile> {
    //! Attempt to download patronage data to a temporary file
    eprintln!("Downloading {:#?}", in_file);
    let mut pat_tmpfile = NamedTempFile::new().context("Error creating temporary file")?;
    let resp = dl_agent
        .get(in_file.to_str().context("utf-8 conversion error")?)
        .call()
        .context("Could not find or download patronage data")?;
    if verbose {
        eprintln!(
            "\t{:#?}\n\t{}{} {}",
            resp.get_url(),
            resp.status(),
            resp.status_text(),
            resp.http_version(),
        );
        for v in resp.headers_names() {
            if let Some(h) = resp.header(&v) {
                eprintln!("\t{v} {h}");
            }
        }
        eprintln!();
    }

    let mut bytes: Vec<u8> = Vec::new();
    resp.into_reader().read_to_end(&mut bytes)?;

    if tree_magic_mini::match_u8("application/zip", &bytes) {
        // this is a zipfile lol
        let cur = Cursor::new(bytes);
        // Responses are merely Read, not Seek.. but Cursors are also Seek...
        let mut zippy = zip::ZipArchive::new(cur).context("Error reading downloaded ZIP")?;
        for i in 0..zippy.len() {
            let mut f = zippy.by_index(i).context("Error unzipping")?;
            if f.enclosed_name()
                .and_then(Path::extension)
                .map_or_else(|| false, |f| f.eq_ignore_ascii_case("csv"))
            {
                eprintln!("{}Extracting: {}", ansi_escapes::CursorPrevLine, f.name());
                std::io::copy(&mut f, &mut pat_tmpfile).context("Error storing Patronage CSV")?;
                break;
            }
        }
    } else if tree_magic_mini::match_u8("text/csv", &bytes) {
        // copy and hope
        pat_tmpfile.write_all(&bytes).context("Error saving patronage data to disk")?;
    } else {
        bail!("Unknown Patronage data format");
    }
    Ok(pat_tmpfile)
}

fn download_gtfs(dl_agent: &Agent, gtfs_dir: &PathBuf, verbose: bool) -> Result<TempDir> {
    //! Attempt download of GTFS data.

    // gotta download the thing
    // if verbose {
    eprintln!("Downloading {:#?}", gtfs_dir);
    // }
    let gtfs_tmpdir = tempfile::tempdir()?;
    let mut tmp_path = gtfs_tmpdir.path().to_path_buf();

    let resp = dl_agent
        .get(gtfs_dir.to_str().context("utf-8 conversion error")?)
        .call()
        .context("Could not find or download patronage data")?;

    if verbose {
        eprintln!(
            "\t{:#?}\n\t{}{} {}",
            resp.get_url(),
            resp.status(),
            resp.status_text(),
            resp.http_version(),
        );
        for v in resp.headers_names() {
            if let Some(h) = resp.header(&v) {
                eprintln!("\t{v} {h}");
            }
        }
    }

    let mut bytes: Vec<u8> = Vec::new();
    resp.into_reader().read_to_end(&mut bytes)?;

    if tree_magic_mini::match_u8("application/zip", &bytes) {
        let cur = Cursor::new(bytes);
        let mut zippy = zip::ZipArchive::new(cur).context("Error reading downloaded ZIP")?;

        for i in 0..zippy.len() {
            let mut zfile = zippy.by_index(i).context("Error reading GTFS ZIP")?;
            tmp_path.push(zfile.name());
            let mut actual = File::create(&tmp_path).context("Error saving GTFS file")?;
            std::io::copy(&mut zfile, &mut actual).context("Error extracting from GTFS ZIP")?;
            tmp_path.pop();
        }
    } else {
        bail!(std::io::Error::from(std::io::ErrorKind::InvalidData));
    }
    Ok(gtfs_tmpdir)
}

fn load_patronage(
    db: &Connection,
    infilename: &Path,
    pat_tmpfile: Option<NamedTempFile>,
) -> Result<()> {
    //! Load patronage data into the database from CSV

    // lack of spaces around = is necessary
    let schema = format!(
        "CREATE VIRTUAL TABLE PInit USING csv(filename='{}', header=YES)",
        infilename.display()
    );
    db.execute_batch(&schema)?;

    let schema = "CREATE TABLE Patronage(operator TEXT, month TEXT, route TEXT, direction TEXT, time TEXT, ticket_type TEXT, origin_stop INTEGER, destination_stop INTEGER, quantity INTEGER);";

    db.execute_batch(schema).context("Failed to create real table.")?;

    let schema = "INSERT INTO Patronage SELECT * FROM PInit;";

    match db
        .execute_batch(schema)
        .context("Read the patronage CSV but could not convert the type affinities.")
    {
        Ok(_) => {}
        Err(e) => {
            if let Some(t) = pat_tmpfile {
                if let Err(b) = t
                    .persist("./error.csv")
                    .context("Error also while attempting to persist CSV for inspection.")
                {
                    // TODO combine with DB execution error too?
                    bail!(anyhow!(e).context(b));
                }
                return Err(e).context("Refer to ./error.csv for more info.");
            }
            return Err(e);
        }
    }

    // Prep an index. This alone practically halved the runtime when added.
    if let Err(e) =
        db.execute_batch("CREATE INDEX idx_patronage_routedir on Patronage(route, direction);")
    {
        eprintln!(
            "Warning: error creating index on patronage database; performance may be degraded\n{}",
            e
        );
    }
    Ok(())
}

fn write_index_html(
    rd_tree: &BTreeMap<String, Vec<String>>,
    out_dir: &Path,
    month: &str,
    year: &str,
    ftime: &Option<String>,
) -> Result<(), anyhow::Error> {
    let mut index_html = format!(
        r#"<html>
<head>
<body>
<h4 style="margin-left: 1vw">{} {}</h4>
<table>
"#,
        convert_monthname(month),
        year
    );

    for (k, v) in rd_tree {
        write!(index_html, "<tr>")?;
        for d in v {
            write!(index_html, r#"<td><a href="{}_{}.svg">{} {}</a></td>"#, k, d, k, d)?;
        }
        writeln!(index_html, "</tr>")?;
    }
    write!(index_html, "</table>\n</body>\n</html>")?;

    write_outfile(out_dir, "index.html", month, year, ftime, &index_html)
        .context("Error writing index.html")
}

/// Write a file to $output/yyyy/mm/[time/]filename
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
    if let Some(f) = ftime.as_ref() {
        outfile.push(f.replace(' ', "_").replace('(', "").replace(')', "").replace(':', ""));
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
        "counterclockwise" | "outbound" | "south" | "west" => "1",
        _ => "0",
    }
}

/// Attempt to convert a month-as-digit to its English name
fn convert_monthname(from: &str) -> &str {
    if let Ok(m) = from.parse::<u8>() {
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
    } else {
        from
    }
}

fn days_per_month(month: &str, year: &str) -> Result<f32> {
    //! Returns the number of days per month (e.g. January = 31)
    //! month and year should be digits, not names... (and January = 1)
    let m: u8 = month.parse()?;
    let y: usize = year.parse()?;
    let leap: bool = (y % 4 == 0) && ((y % 100 != 0) || (y % 400 == 0));

    Ok(match m {
        9 | 4 | 6 | 11 => 30.0, // (1) 30 days hath September, April, June and November,
        2 => {
            if leap {
                // (3) except February,
                29.0 // (3b) and 29 days each leap year.
            } else {
                28.0 // (3a) which has 28 days clear,
            }
        }
        _ => 31.0, // (2) all the rest have 31,
    })
}
