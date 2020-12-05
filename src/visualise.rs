//! Perform the actual visualisation

use std::collections::BTreeMap;

use hsluv::*;
use rand::Rng;
use std::path::PathBuf;

// spacing constants
const SPACE: f64 = 50.0;
const BETWEEN: f64 = 2.5 * SPACE;
const MIN_GAP: f64 = 0.5 * SPACE;
const TEXT_SECTION: f64 = 11.0 * SPACE;
const EXTRA: f64 = 2.0 * SPACE;

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

    let len = input.len();

    if len < 2 {
        return input;
    } else if len == 4 {
        return vec![
            input[1].clone(),
            input[3].clone(),
            input[0].clone(),
            input[2].clone(),
        ];
    } else if len == 6 {
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
        return out;
    }
}

fn sum_up(patronages: &BTreeMap<(i32, i32), i32>) -> (BTreeMap<i32, i32>, BTreeMap<i32, i32>) {
    //! {(origin_stop : patronage} and {destination_stop : patronage}
    let mut boardings: BTreeMap<i32, i32> = BTreeMap::new();
    let mut alightings: BTreeMap<i32, i32> = BTreeMap::new();

    for (k, qty) in patronages {
        let from: i32 = k.0;
        let to: i32 = k.1;

        let fq: i32 = *boardings.get(&from).unwrap_or(&0);
        let tq: i32 = *alightings.get(&to).unwrap_or(&0);

        boardings.insert(from, qty + fq);
        alightings.insert(to, qty + tq);
    }
    return (boardings, alightings);
}

fn make_css(
    swap_colours: bool,
    jumble_colours: bool,
    css_path: Option<PathBuf>,
    stop_count: usize,
) -> std::io::Result<String> {
    //! Construct CSS including its colour list.

    // 1. load CSS
    let mut css = match css_path {
        Some(p) => std::fs::read_to_string(p)?,
        None => String::from(include_str!("default.css")),
    };
    // 2. create colour list
    let colours = match jumble_colours {
        true => jumbled(colour_list(stop_count)),
        false => colour_list(stop_count),
    };
    // put colours into CSS
    for k in 0..stop_count {
        let colour_by = match swap_colours {
            true => "t",
            false => "f",
        };
        css.push_str(&format!(".{}{} {{stroke: {}}}\n", colour_by, k, colours[k]));
    }

    Ok(css)
}

pub fn visualise_one(
    patronages: BTreeMap<(i32, i32), i32>,
    stop_sequence: Vec<i64>,
    stop_names: BTreeMap<i64, String>,
    service_count: i32,
    route_name: &str,
    direction: &str,
    month: &str,
    year: &str,
    swap_colours: bool,
    jumble_colours: bool,
    css_path: Option<PathBuf>,
) -> std::io::Result<String> {
    // we need to sum boardings and alightings for each stop_id so we know how wide to make arcs
    let (boardings, alightings) = sum_up(&patronages);

    // and now we know what the ultimate sequence number of everything is...
    let mut seqi: BTreeMap<i64, usize> = BTreeMap::new();
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

    let boarding_max = boardings.values().cloned().max().unwrap_or(0);
    let alighting_max = alightings.values().cloned().max().unwrap_or(0);

    let tots_max = (boarding_max + alighting_max) as f64 * SPACE / (BETWEEN - MIN_GAP);

    let mut midline = format!(
        r#"<line id="mainline" x1="{}" x2="{}" y1="{}" y2="{}" />"#,
        EXTRA,
        EXTRA + (stop_count as f64 - 1.0) * BETWEEN,
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
            let quantity = *patronages.get(&(from as i32, to as i32)).unwrap_or(&0);
            if quantity < 1 {
                continue;
            }
            // can just sum this all up for the wraparounds
            current_load += quantity;

            let alt_txt = format!(
                "from: {}\nto: {}\npassengers: {}",
                from_name, to_name, quantity
            );

            let y1 = main_height;
            let y2 = y1;
            let width = SPACE * quantity as f64 / tots_max;

            // need to figure out arcs in/out of the page, and have two of them - "wrap around"
            // these need to be two-arc paths!
            let tst = dest_subtotals[to_idx];
            let ost = orig_subtotals[from_idx];

            let x1_right = (EXTRA + from_idx as f64 * BETWEEN) + (ost + width / 2.0) + SPACE / 50.0;
            let x2_left = (EXTRA + to_idx as f64 * BETWEEN) - (width / 2.0 + tst + SPACE / 50.0);
            let x2_right = x2_left + (stop_count as f64 * BETWEEN);
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

        let orig_total = SPACE * (*boardings.get(&(from as i32)).unwrap_or(&0) as f64) / tots_max;

        // we're going outside-in here so the wraparound subtotals aren't relevant to us
        // and due to how we iterate, we only need the scalar here
        let mut orig_subtotal = 0.0;

        for to_idx in (from_idx + 1)..stop_count {
            let to = stop_sequence[to_idx];
            let tostr = to.to_string();
            let to_name = stop_names.get(&to).unwrap_or(&tostr);
            let quantity = *patronages.get(&(from as i32, to as i32)).unwrap_or(&0);
            if quantity == 0 {
                continue;
            }

            let alt_txt = format!(
                "from: {}\nto: {}\npassengers: {}",
                from_name, to_name, quantity
            );

            // now we need to construct our path coordinates
            let y1 = main_height;
            let y2 = y1;
            let width = SPACE * quantity as f64 / tots_max;

            let dst = dest_subtotals[to_idx];

            let x1 = (EXTRA + from_idx as f64 * BETWEEN)
                + (orig_total - (orig_subtotal + width / 2.0))
                + SPACE / 50.0;
            let x2 = (EXTRA + to_idx as f64 * BETWEEN) - (width / 2.0 + dst + SPACE / 50.0);

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

        let alights = *alightings.get(&(from as i32)).unwrap_or(&0);
        let boards = *boardings.get(&(from as i32)).unwrap_or(&0);

        // label things
        let line2 = format!("{} alightings | {} boardings", alights, boards);
        let t_x = EXTRA + (from_idx as f64 * BETWEEN) - SPACE / 8.0;
        let t_y = main_height + SPACE / 2.0;
        let t_y2 = t_y + SPACE / 2.0;

        for t_c in ["keyline", "foreground"].iter() {
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

        let b_x = EXTRA + (from_idx as f64) * BETWEEN + BETWEEN / 2.0;
        let b_y1 = doc_height;
        let b_y2 = doc_height - ((SPACE * (current_load as f64)) / tots_max);

        let bar = format!(
            r#"<line class="bargraph" stroke-width="{}" x1="{}" x2="{}" y1="{}" y2="{}" />"#,
            BETWEEN, b_x, b_x, b_y1, b_y2
        );
        bargraph.push_str(&bar);

        let mut loopy = "";
        if from_idx + 1 == stop_count {
            loopy = "&#8634; ";
        }

        for t_c in ["keyline", "foreground"].iter() {
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
            (EXTRA + from_idx as f64 * BETWEEN),
            main_height
        );
        midline.push_str(&circ);
    }

    let boards_count: i32 = boardings.values().sum();
    let title = format!(
        r#"<text class="title" x="{}" y="100">{} {} – {} {}</text>
    <text class="subtitle" x="{}" y="150">{} services</text>"#, // {} boardings TODO
        doc_width / 2.0,
        route_name,
        direction,
        month,
        year,
        doc_width / 2.0,
        //boards_count,
        service_count
    );

    // need to write instead of print, eventually
    Ok(format!(
        r#"<?xml version="1.0" encoding="utf-8" ?>
<svg baseProfile="full" height="{}" version="1.1" width="{}"
     xmlns="http://www.w3.org/2000/svg">
    <defs><style type="text/css"><![CDATA[
    {}]]>
    </style></defs>
    <rect height="100%" id="bgrect" width="100%" x="0" y="0" />
    {}
    {}
    {}
    {}
    {}
    {}
</svg>"#,
        doc_height, doc_width, css, paths_rev, paths_fwd, labels, bargraph, midline, title
    ))
}
