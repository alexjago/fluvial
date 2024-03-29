//! Perform the actual visualisation

use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;

use anyhow::Result;
use hsluv::hsluv_to_hex;
use rand::Rng;
use std::path::PathBuf;

use crate::gtfs::{Quantity, StopId};

// spacing constants
/// Spacing unit in pixels
const SPACE: f64 = 50.0;
/// Distance between stops
const BETWEEN: f64 = 2.5 * SPACE;
/// Minimum distance
const MIN_GAP: f64 = 0.5 * SPACE;
/// Height of the text section
const TEXT_SECTION: f64 = 11.0 * SPACE;
/// Edge padding
const EXTRA: f64 = 2.0 * SPACE;

fn colour_list(count: usize) -> Vec<String> {
    //! Create a vector of hex colour codes, with evenly-spaced hues
    //! plus a little bit of variance in saturation and lightness.
    let mut out = Vec::<String>::new();
    let mut rng = rand::thread_rng();
    for k in 0..count {
        let hue = 360.0 * (k as f64) / (count as f64);
        let sat_var: f64 = rng.gen();
        let sat = 10.0f64.mul_add(sat_var, 90.0);
        let val = match k % 4 {
            1 => 50.0,
            3 => 60.0,
            _ => 55.0,
        };
        out.push(hsluv_to_hex((hue, sat, val)));
    }
    out
}

fn jumbled<T>(input: Vec<T>) -> Vec<T>
where
    T: Clone,
{
    //! If `input.len() >= 2`, returns a copy of `input` with its elements permuted in a star pattern.
    //! Otherwise, returns `input`.

    let len = input.len();

    if len < 2 {
        input
    } else if len == 4 {
        vec![input[1].clone(), input[3].clone(), input[0].clone(), input[2].clone()]
    } else if len == 6 {
        vec![
            input[1].clone(),
            input[3].clone(),
            input[5].clone(),
            input[0].clone(),
            input[2].clone(),
            input[4].clone(),
        ]
    } else {
        // want g, L to be co-prime for a star pattern
        // if m, n are coprime then more coprime pairs can be generated:
        // (2m - n, m) and (2m + n, m) and (m + 2n, n)
        // always coprime if m = n - 1 (for m >= 3)
        let g = match len % 3 {
            1 => len / 3, // (m + 2n, n) => 3k + 1, k = n
            _ => match len % 9 {
                6 => len / 3 - 1, // 3(3k-1) e.g. 15 so L/3 + 1 is div. by 3
                _ => len / 3 + 1, // (2m + n, m) => 3k + 2, k = n AND ALSO other 3k's
            },
        };

        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            out.push(input[(g * (i + g)) % len].clone());
        }
        out
    }
}

fn sum_up(
    patronages: &BTreeMap<(StopId, StopId), Quantity>,
) -> (BTreeMap<StopId, Quantity>, BTreeMap<StopId, Quantity>) {
    //! {(`origin_stop` : patronage} and {`destination_stop` : patronage}
    let mut boardings = BTreeMap::new();
    let mut alightings = BTreeMap::new();

    for (k, qty) in patronages {
        let from = k.0;
        let to = k.1;

        let fq = *boardings.get(&from).unwrap_or(&0);
        let tq = *alightings.get(&to).unwrap_or(&0);

        boardings.insert(from, qty + fq);
        alightings.insert(to, qty + tq);
    }
    (boardings, alightings)
}

fn make_css(
    swap_colours: bool,
    jumble_colours: bool,
    css_path: &Option<PathBuf>,
    stop_count: usize,
) -> Result<String> {
    //! Construct CSS including its colour list.

    // 1. load CSS
    let mut css = match css_path.as_ref() {
        Some(p) => std::fs::read_to_string(p)?,
        None => String::from(include_str!("default.css")),
    };
    // 2. create colour list
    let colours =
        if jumble_colours { jumbled(colour_list(stop_count)) } else { colour_list(stop_count) };
    // put colours into CSS
    for (k, colour) in colours.iter().enumerate().take(stop_count) {
        let colour_by = if swap_colours { "t" } else { "f" };
        writeln!(css, ".{}{} {{stroke: {}}}", colour_by, k, colour)?;
    }

    Ok(css)
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
/// Visualise a single route.
/// Stops are laid out left-right in order of `stop_sequence`
/// Arcs are drawn between stops according to `patronages`
/// etc, etc
pub fn visualise_one(
    patronages: &BTreeMap<(StopId, StopId), Quantity>,
    stop_sequence: &Vec<StopId>,
    stop_names: &BTreeMap<StopId, String>,
    service_count: Quantity,
    route_name: &str,
    direction: &str,
    ftime: &Option<String>,
    month: &str,
    year: &str,
    swap_colours: bool,
    jumble_colours: bool,
    css_path: &Option<PathBuf>,
) -> Result<String> {
    // we need to sum boardings and alightings for each stop_id so we know how wide to make arcs
    let (boardings, alightings) = sum_up(patronages);

    // and now we know what the ultimate sequence number of everything is...
    let mut seqi: BTreeMap<StopId, usize> = BTreeMap::new();
    for (i, k) in stop_sequence.iter().enumerate() {
        seqi.insert(*k, i);
    }

    let stop_count = stop_sequence.len();
    let css = make_css(swap_colours, jumble_colours, css_path, stop_count)?;

    let mut paths_fwd = String::new();
    let mut paths_rev = String::new();
    let mut labels = String::new();
    let mut bargraph = String::new();

    // hardcode worst-case for now, do something more intelligent later
    let tier_max = stop_count as f64;

    // other dimensions
    let main_height = (tier_max + 2.0) * 0.5 * BETWEEN;
    let main_width = (stop_count as f64 - 1.0) * BETWEEN;
    let doc_width = EXTRA + main_width + EXTRA;
    let doc_height = TEXT_SECTION + main_height;

    let boarding_max = boardings.values().copied().max().unwrap_or(0);
    let alighting_max = alightings.values().copied().max().unwrap_or(0);

    let tots_max = f64::from(boarding_max + alighting_max) * SPACE / (BETWEEN - MIN_GAP);

    let mut midline = format!(
        r#"<line class="mainline" x1="{}" x2="{}" y1="{}" y2="{}" />"#,
        EXTRA,
        (stop_count as f64 - 1.0).mul_add(BETWEEN, EXTRA),
        main_height,
        main_height
    );

    /* current_load calculations are a bit more complicated now that we go "through the loop".
     * Current load is defined *between* stops, as the sum of the number of people in all
     * arcs passing overhead. Luckily, since we have to evaluate wrap-arounds separately anyway
     * we can just sum up all that patronage and know it will be accurate going in to the
     * regular sequence... and then just subtract alightings and add boardings like normal.
     */

    let mut current_load = 0;

    /* Reverse arc layout has to be in a separate set of loops to forwards arc layout
     * This is a little unfortunate due to the code duplication
     * Reverse arcs need to be laid out in the following order (numbers are indexes):
     * 1->0, 2->1, ... 2->0, 3->1, ... k-1 -> 0, where k is the number of stops
     * But forward arcs want a different order: 0 -> 1, 0 -> 2, ... 1 -> 2, ... k-2 -> k-1.
     * (Reverse arcs lay out tallest-first, forwards arcs lay out shortest-first).
     */

    let mut orig_subtotals: Vec<f64> = vec![0.0; stop_count];
    let mut dest_subtotals: Vec<f64> = vec![0.0; stop_count];

    for offset in 1..stop_count {
        for to_idx in 0..(stop_count - offset) {
            let from_idx = to_idx + offset;
            let to = stop_sequence[to_idx];
            let from = stop_sequence[from_idx];

            let fromstr = from.to_string();
            let from_name = stop_names.get(&from).unwrap_or(&fromstr);

            let tostr = to.to_string();
            let to_name = stop_names.get(&to).unwrap_or(&tostr);
            let quantity = *patronages.get(&(from, to)).unwrap_or(&0);
            if quantity < 1 {
                continue;
            }
            // can just sum this all up for the wraparounds
            current_load += quantity;

            let alt_txt = format!("from: {}\nto: {}\npassengers: {}", from_name, to_name, quantity);

            let y1 = main_height;
            let y2 = y1;
            let width = SPACE * f64::from(quantity) / tots_max;

            // need to figure out arcs in/out of the page, and have two of them - "wrap around"
            // these need to be two-arc paths!
            let to_dest = dest_subtotals[to_idx];
            let from_orig = orig_subtotals[from_idx];

            let x1_right =
                (from_idx as f64).mul_add(BETWEEN, EXTRA) + (from_orig + width / 2.0) + SPACE / 50.0;
            let x2_left =
                (to_idx as f64).mul_add(BETWEEN, EXTRA) - (width / 2.0 + to_dest + SPACE / 50.0);
            let x2_right = (stop_count as f64).mul_add(BETWEEN, x2_left);
            let x1_left = x1_right - (stop_count as f64 * BETWEEN);

            let path = format!(
                r#"<path class="arc f{} t{}" d="M{:.5} {} v{} A 1 1 0 1 1 {:.5} {} v{} M{:.5} {} v{} A1 1 0 1 1 {:.5} {} v{}" stroke-width="{:.5}"><title>{}</title></path>
                "#,
                from_idx,
                to_idx,
                x1_right,
                doc_height,
                -TEXT_SECTION,
                x2_right,
                y2,
                TEXT_SECTION,
                x1_left,
                doc_height,
                -TEXT_SECTION,
                x2_left,
                y2,
                TEXT_SECTION,
                width,
                alt_txt
            );
            paths_rev.push_str(&path);

            orig_subtotals[from_idx] += width;
            dest_subtotals[to_idx] += width;
        }
    }

    /* Now the "forward-arc" case
     * Our visiting order here is of course 0 -> 1, 0 -> 2, ... 1 -> 2, ... k-2 -> k-1
     */
    for from_idx in 0..stop_count {
        let from = stop_sequence[from_idx];
        let fromstr = from.to_string();
        let from_name = stop_names.get(&from).unwrap_or(&fromstr);

        let orig_total = SPACE * f64::from(*boardings.get(&from).unwrap_or(&0)) / tots_max;

        // we're going outside-in here so the wraparound subtotals aren't relevant to us
        // and due to how we iterate, we only need the scalar here
        let mut orig_subtotal = 0.0;

        for to_idx in (from_idx + 1)..stop_count {
            let to = stop_sequence[to_idx];
            let tostr = to.to_string();
            let to_name = stop_names.get(&to).unwrap_or(&tostr);
            let quantity = *patronages.get(&(from, to)).unwrap_or(&0);
            if quantity == 0 {
                continue;
            }

            let alt_txt = format!("from: {}\nto: {}\npassengers: {}", from_name, to_name, quantity);

            // now we need to construct our path coordinates
            let y1 = main_height;
            let y2 = y1;
            let width = SPACE * f64::from(quantity) / tots_max;

            let dst = dest_subtotals[to_idx];

            let x1 = (from_idx as f64).mul_add(BETWEEN, EXTRA)
                + (orig_total - (orig_subtotal + width / 2.0))
                + SPACE / 50.0;
            let x2 = (to_idx as f64).mul_add(BETWEEN, EXTRA) - (width / 2.0 + dst + SPACE / 50.0);

            let path = format!(
                r#"<path class="arc f{} t{}" d="m{:.5} {} v{} A1 1 0 1 1 {:.5} {} v{}" stroke-width="{:.5}"><title>{}</title></path>
        "#,
                from_idx,
                to_idx,
                x1,
                doc_height,
                -TEXT_SECTION,
                x2,
                y2,
                TEXT_SECTION,
                width,
                alt_txt
            );
            paths_fwd.push_str(&path);
            dest_subtotals[to_idx] += width;

            orig_subtotal += width;
        }

        let alights = *alightings.get(&from).unwrap_or(&0);
        let boards = *boardings.get(&from).unwrap_or(&0);

        // label things
        let line2 = format!("{} alightings | {} boardings", alights, boards);
        let t_x = (from_idx as f64).mul_add(BETWEEN, EXTRA) - SPACE / 8.0;
        let t_y = main_height + SPACE / 2.0;
        let t_y2 = t_y + SPACE / 2.0;

        for t_c in &["keyline", "foreground"] {
            let label_txt = format!(
                r#"<text class="{t_c}" font-size="25.0" text-anchor="end" transform="rotate(270,{t_x},{t_y})" x="{t_x}" y="{t_y}">{from_name}<tspan x="{t_x}" y="{t_y2}">{line2}</tspan></text>"#,
                t_c = *t_c,
                t_x = t_x,
                t_y = t_y,
                from_name = from_name,
                t_y2 = t_y2,
                line2 = line2
            );
            labels.push_str(&label_txt);
        }

        // bargraph things
        current_load -= alights;
        current_load += boards;

        let b_x = (from_idx as f64).mul_add(BETWEEN, EXTRA) + BETWEEN / 2.0;
        let b_y1 = doc_height;
        let b_y2 = doc_height - ((SPACE * f64::from(current_load)) / tots_max);

        let bar = format!(
            r#"<line class="bargraph" stroke-width="{}" x1="{}" x2="{}" y1="{}" y2="{}" />"#,
            BETWEEN, b_x, b_x, b_y1, b_y2
        );
        bargraph.push_str(&bar);

        let loopy = if from_idx + 1 == stop_count {
            // anticlockwise open circle arrow
            "&#8634; "
        } else {
            ""
        };

        for t_c in &["keyline", "foreground"] {
            let bt = format!(
                r#"<text class="{} bartxt" text-anchor="middle" x="{}" y="{}">{}{}</text>"#,
                *t_c,
                b_x,
                b_y1 - SPACE / 5.0,
                loopy,
                current_load
            );
            bargraph.push_str(&bt);
        }

        // circle markers
        let circ = format!(
            r#"<circle class="markers" cx="{}" cy="{}" r="12.5" />"#,
            (from_idx as f64).mul_add(BETWEEN, EXTRA),
            main_height
        );
        midline.push_str(&circ);
    }

    let ftime_ins = match ftime.as_ref() {
        Some(s) => format!("; {}", s),
        None => String::new(),
    };

    let boards_count: Quantity = boardings.values().sum();
    #[allow(clippy::non_ascii_literal)]
    let title = format!(
        r#"<text class="title" x="{}" y="100">{} {} – {} {}</text>
    <text class="subtitle" x="{}" y="150">{} boardings; est. {} services{}</text>"#, // {} services TODO
        doc_width / 2.0,
        route_name,
        direction,
        month,
        year,
        doc_width / 2.0,
        boards_count,
        service_count,
        ftime_ins
    );

    Ok(format!(
        // glorious hack: include_str! is eagerly evaluated
        include_str!("template.svg"),
        doc_width, doc_height, css, paths_rev, paths_fwd, labels, bargraph, midline, title
    ))
}
