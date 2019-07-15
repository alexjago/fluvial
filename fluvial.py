#!/usr/bin/env python3

## Fluvial
## A bit like a Sankey diagram, only a little simpler.
## Intended for visualising passenger flows over a route.
## CSV as input: origin tag, destination tag, quantity

# version 2, effectively

## Dependencies:

# https://github.com/hsluv/hsluv-python
# pip install hsluv
# for proper diverse colour generation

# https://github.com/mozman/svgwrite
# pip install svgwrite
# For good and proper SVG generation

import hsluv
import svgwrite
import random
import csv
import argparse
import sys
import math
from functools import cmp_to_key
import os.path
import shutil
import shlex
import os
import subprocess
from collections import Counter
import statistics
import pickle
import configparser
import tempfile


__VERBOSE = True # nasty globals, we hates it, we hates it

# Let's generate some colours!
# (this is the only thing we need hsluv for)
def colour_list(count, start=0, stop=360, chroma=100, lightness=50, chroma_var=5, lightness_var=5):
    out = []
    for c in range(count):
        # it's helpful to have some saturation and chroma variance.
        hue = start + c * stop/count
        chroma -= chroma_var*random.random()
        lightness += lightness_var * [0,1,0,-1][c%4]
        out.append(hsluv.hsluv_to_hex([hue, chroma, lightness]))
    return out


def jumbled(c_list):
    """Returns a jumbled list of colours from their initial order"""
    # we desire different-coloured neighbours with a star-style visiting strategy
    # We want g < L such that g and L are coprime, then g*k % L visits everything
    L = len(c_list)
    g = int((L-1)/3)

    if L>6 and L % g:
        # go by thirds; RGB and all that
        out = []
        for i in range(len(c_list)):
            out.append(c_list[ (g*(i+g)) % L ])
            # bonus +g to make the start different
        return out

    elif L > 2:
        return c_list[1:L:3] + c_list[2:L:3] + c_list[0:L:3]
        # ^^ odds, evens, three-vens

    else:
        return c_list # give up



def listmerge(a, b):
    """Merge two similar lists positionally"""
    i = 0
    j = 0
    out = []

    while (i < len(a)) and (j < len(b)) :
        if a[i] == b[j]:
            out.append(a[i])
            i += 1
            j += 1
        elif a[i] in b[j:]:
            out.append(b[j])
            j += 1
        elif b[j] in a[i:]:
            out.append(a[i])
            i += 1
        else: # fallback: alternate
            out.append(a[i])
            out.append(b[j])
            i += 1
            j += 1

    if i < len(a):
        out += a[i:]
    elif j < len(b):
        out += b[j:]
    # #end while#
    return out
# #end listmerge#


def printv(*args, **kwargs):
    if __VERBOSE:
        print(*args, **kwargs, file=sys.stderr)


def get_route_stop_avgs(gtfs_dir, gtfs_cache):
    """Sort our GTFS stuff out.
       Returns a dict keyed by (stop_id, route_short_name, direction_id)"""

    # zeroth check if we've done this already, and that xsv exists
    if not os.path.isdir(gtfs_dir):
        print(f"Missing GTFS; {gtfs_dir} is not a directory.", file=sys.stderr)
        exit(1) #bye

    fluvdir = gtfs_cache
    routestops_avg = os.path.join(fluvdir, 'route_stops_avg.pickle')
    routestops_csv = os.path.join(fluvdir, 'route_stops.csv')
    triptimes = os.path.join(fluvdir, "trip_times.csv")

    if os.path.exists(routestops_avg):
        with open(routestops_avg, 'rb') as rsa:
            return pickle.load(rsa)
    else:
        printv(f"Generating: {routestops_avg}")

        if not shutil.which('xsv'):
            print("Missing `xsv`; get it at https://github.com/BurntSushi/xsv", file=sys.stderr)
            exit(1)
        # Make a fluvial subdir there, for outputs/intermediaries
        if not os.path.exists(fluvdir):
            os.mkdir(fluvdir)

        if not os.path.exists(triptimes):
            # Jump straight into the big boi join
            #   xsv join trip_id {stop_times.txt} trip_id {trips.txt}
            #   | xsv select -o {trip_times.csv} stop_id,stop_sequence,route_id,direction_id
            stoptimes_txt = os.path.join(gtfs_dir, 'stop_times.txt')
            trips_txt = os.path.join(gtfs_dir, 'trips.txt')

            join1 = subprocess.Popen(['xsv', 'join', 'trip_id', stoptimes_txt, 'trip_id', trips_txt], stdout=subprocess.PIPE)
            #sel1  = subprocess.Popen(['xsv', 'select', 'stop_id,stop_sequence,route_id,direction_id'], stdin=join1.stdout, stdout=subprocess.PIPE)
            sel1a  = subprocess.Popen(['xsv', 'select', '-o', triptimes, 'stop_id,stop_sequence,route_id,direction_id'], stdin=join1.stdout)
            # also, sort-unique drops the filesize by 50x post-selection...
            # but should we really be doing it? That's actually data there.
            #sort1 = subprocess.Popen(['xsv', 'sort'], stdin=sel1.stdout, stdout=subprocess.PIPE)
            #uniq1 = subprocess.Popen(['uniq', '-', triptimes], stdin=sort1.stdout)
            join1.stdout.close()
            #sel1.stdout.close()
            sel1a.wait()
            #sort1.stdout.close()
            #uniq1.wait() # we need triptimes on-disk, it seems.


        if not os.path.exists(routestops_csv):
            # and then
            #   xsv join route_id {triptimes.csv} route_id routes.txt
            #   | xsv select stop_id,route_short_name,direction_id,stop_sequence

            routes_txt = os.path.join(gtfs_dir, 'routes.txt')

            join2 = subprocess.Popen(['xsv', 'join', 'route_id', triptimes, 'route_id', routes_txt], stdout=subprocess.PIPE)
            sel2 = subprocess.Popen(['xsv', 'select', '-o', routestops_csv, 'stop_id,route_short_name,direction_id,stop_sequence'], stdin=join2.stdout)
            join2.stdout.close()
            sel2.wait()

        combine = {} # (multikeys) : Counter

        with open(routestops_csv, 'r') as infile:

            thisdialect = csv.Sniffer().sniff(infile.read(1024))
            infile.seek(0)
            rdr = csv.reader(infile, dialect=thisdialect)
            next(rdr) # skip header
            # iteration
            for row in rdr:
                keyme = tuple(row[:3]) # because we know we just wrote the header as 'stop_id,route_short_name,direction_id,stop_sequence'

                if not keyme in combine:
                    combine[keyme] = Counter()

                combine[keyme][int(row[3])] += 1

        # with that out of the way
        averages = {k: round(statistics.mean(v.elements()), 2) for k,v in combine.items()}

        with open(routestops_avg, 'wb') as rsa:
            pickle.dump(averages, rsa, protocol=4)

        with open(os.path.join(fluvdir, 'route_stops_avg.csv'), 'w') as rsa:
            wrt = csv.writer(rsa)
            wrt.writerow(['stop_id','route_short_name','direction_id','stop_sequence_avg'])
            for k,v in averages.items():
                wrt.writerow([*k] + [v])


        return averages

    # and we should be almost done with all our GTFS weirdness.
    # (bar generating the stop_name look-up table)
    # Further processing should be searching and after #


def get_stop_names(gtfs_dir, gtfs_cache):
    """Returns a {stop_id: stop_name} dict ultimately generated from stops.txt"""

    if not os.path.isdir(gtfs_dir):
        print(f"Missing GTFS; {gtfs_dir} is not a directory.", file=sys.stderr)
        exit(1) #bye

    fluvdir = gtfs_cache
    if not os.path.exists(fluvdir):
        printv(fluvdir, "missing! This should already exist...")
        os.mkdir(fluvdir)

    stop_names_pickle = os.path.join(fluvdir, "stop_names.pickle")

    if os.path.exists(stop_names_pickle):
        with open(stop_names_pickle, 'rb') as snp:
            return pickle.load(snp)
    else:
        # load, filter, save
        stops_txt = os.path.join(gtfs_dir, "stops.txt")

        stop_names = {}

        with open(stops_txt) as infile:
            thisdialect = csv.Sniffer().sniff(infile.read(1024))
            infile.seek(0)
            rdr = csv.DictReader(infile, dialect=thisdialect)

            stop_names = {r['stop_id']:r['stop_name'] for r in rdr}

        with open(stop_names_pickle, 'wb') as snp:
            pickle.dump(stop_names, snp, protocol=4)
            return stop_names



def get_route_direction_pairs(infile, route_h, direction_h):
    printv("Getting route/direction pairs...")
    infile.seek(0)
    out = set()

    thisdialect = csv.Sniffer().sniff(infile.read(1024))
    infile.seek(0)
    rdr = csv.DictReader(infile, dialect=thisdialect)
    for r in rdr:
        out.add((r[route_h], r[direction_h]))

    infile.seek(0)

    printv("Found {} route/direction pairs".format(len(out)))
    return list(out)


def get_posfile_stop_info(posfile, column):
    """Returns a dictionary of {stop_id:column}"""

    if not os.path.exists(posfile):
        print("Missing positions file", posfile, file=sys.stderr)
        exit(1)

    thisdialect = csv.Sniffer().sniff(posfile.read(1024))
    posfile.seek(0)
    rdr = csv.DictReader(posfile, dialect=thisdialect)

    out = {r['stop_id']:r[column] for r in rdr}
    posfile.seek(0)
    return out


# On to the main thing

def process(opts, routename, routedirection):

    print(routename, routedirection)
    # big pile of things that we shall soon fill up
    tree = {}
    total_ogs = {}
    total_dests = {}
    qt_max = 0 # largest individual thing
    tots_max = 0 # largest total (more useful given presentation)
    positions = [] # ultimate ordering repository
    positions_ids = []
    positions_og = [] # origins in 'encounter order'
    positions_ds = [] # destinations in 'encounter order'
    positions_set = set()
    positions_dict = {} # (stop_id, route_short_name, direction_id) : position
    stop_names = {}

    if opts['gtfs_dir']:
        positions_dict = get_route_stop_avgs(opts['gtfs_dir'], opts['gtfs_cache'])
        stop_names = get_stop_names(opts['gtfs_dir'], opts['gtfs_cache'])
    elif opts['positions_file']:
        positions_dict = get_posfile_stop_info(opts['positions_file'], 'stop_sequence')
        stop_names = get_posfile_stop_info(opts['positions_file'], 'stop_name')

    # normally we'd go 'with opts['infile'] as infile
    # but we don't want the context manager closing it on us
    infile = opts['infile']
    infile.seek(0)
    thisdialect = csv.Sniffer().sniff(infile.read(1024))
    infile.seek(0)
    rdr = csv.reader(infile, thisdialect)
    raw_rows = [r for r in rdr]

    # Column indexes
    pfh = raw_rows[0] # patronage file header
    printv(pfh)
    og = pfh.index(opts['INFILE_HEADERS']['origin_stop'])
    ds = pfh.index(opts['INFILE_HEADERS']['destination_stop'])
    qt = pfh.index(opts['INFILE_HEADERS']['quantity'])
    rn = pfh.index(opts['INFILE_HEADERS']['route'])
    dn = pfh.index(opts['INFILE_HEADERS']['direction'])

    printv(f"og {og},  ds {ds},  qt {qt},  rn {rn},  dn {dn}")
    rdlp = routedirection.lower().replace(' ', '_')

    row_count = 0

    for r in raw_rows[1:]:

        if (r[rn] != routename) or (r[dn] != routedirection):
            continue
        else:
            cont_flag = False
            for k,v in opts['INFILE_FILTERS'].items():
                if r[pfh.index(opts['INFILE_HEADERS'][k])] != v:
                    cont_flag = True # can't `continue` right away, because inner loop
                    break
            if cont_flag:
                continue
        row_count += 1
        #printv(r, row_count)

        if (not opts['reflexive']) and (r[og] == r[ds]):
            continue # i.e. skip

        if opts['gtfs_dir']:
            try:
                positions_set.add((r[og], positions_dict[(r[og], routename, opts['DIRECTIONS'][rdlp])]))
                positions_set.add((r[ds], positions_dict[(r[ds], routename, opts['DIRECTIONS'][rdlp])]))
            except KeyError as e:
                # if this happens it's probably because the route isn't in the GTFS,
                # because it's a school stop or because it's been abolished
                printv(f"Error while processing {routename} {routedirection}, skipping stop", e)
                continue #bye
        else:
            if not r[og] in positions_og:
                positions_og.append(r[og])
            if not r[ds] in positions_ds:
                positions_ds.append(r[ds])

        if r[og] in tree:
            tree[r[og]][r[ds]] = int(r[qt]) + tree[r[og]].get(r[ds], 0) # the += is rather important...
        else:
            tree[r[og]] = {r[ds]: int(r[qt])}

        total_ogs[r[og]] = int(r[qt]) + total_ogs.get(r[og], 0)
        total_dests[r[ds]] = int(r[qt]) + total_dests.get(r[ds], 0)


        # tracking down that empty space
        if not r[og].strip() or not r[ds].strip():
            printv("empty field!", r)

        qt_max = max(qt_max, int(r[qt]))


#        # enough of the input file
    infile.seek(0)
    # ^^^ put it back the way we found it

    if len(total_ogs) == 0:
        printv(routename, "ended up empty.")
        return

    if opts['gtfs_dir'] or opts['positions_file']:
        _sorted_set = sorted(positions_set, key=lambda x: float(x[1]))
        positions = [i[0] for i in _sorted_set]
        position_names = [stop_names.get(i, '--') for i in positions]
        printv("Positions", position_names, sep='\n')
    else: # merge OG and DS
        positions = listmerge(positions_og, positions_ds)

    printv("positions:", positions)

    # We need this one to figure out how tall to make it
    # Sadly quadratic (still, N generally < 30 so not too bad)
    tier_max = 0
    for k,v in tree.items():
        for k2 in v.keys():
            tier_max = max(positions.index(k2) - positions.index(k) , tier_max)

    printv({stop_names[k]: v for k,v in total_ogs.items()})
    printv({stop_names[k]: v for k,v in total_dests.items()})
    printv(f"row_count {row_count}")
    tots_max_og = max([v for k,v in total_ogs.items()])
    tots_max_ds = max([v for k,v in total_dests.items()])

    printv(f"qt_max {qt_max},  tier_max {tier_max},  tots_max {tots_max}")


    space = 50 # spacing baseblock. In... pixels, probably
    between = 2.5*space  # distance between stops
    min_gap = 0.5*space
    text_section = 11*space # below (named for the labels)
    left_extra = 2*space
    right_extra = 2*space
    main_height = (tier_max + 2)*0.5*between
    main_width = (len(positions)-1)*between

    docwidth = (left_extra + main_width + right_extra)
    docheight = (text_section + main_height)

    dwg = svgwrite.Drawing(os.path.join(opts['outdir'], f"{routename}_{routedirection}.svg"), size=(docwidth, docheight))

    dwg.add(dwg.rect((0,0), ("100%", "100%"), id_="bgrect")) # just so there's an easily defined background


    # Let's Get Stylish!
    styles = ""
    if opts['css']:
        # We'd like to use:
        ##dwg.add_stylesheet(opts['css'], "fluvial_stylesheet")
        # but lots of things don't support external stylesheets (cough, Inkscape, cough)
        # So we embed it instead.
        with open(opts['css']) as optcss:
            styles = optcss.read().strip()
    else: # sensible defaults if they aren't provided
        styles = """
                    #bgrect {fill:white; stroke:white}
                    text {font-family: sans-serif; font-size: 1.3em;}
                    .arc {opacity:0.6; fill:none;}
                    .bargraph {opacity: 0.3; stroke:black}
                    .foreground .keyline {fill: black}
                    .keyline {stroke:white; stroke-width:0.5em; stroke-linejoin:round; opacity:0.6}
                    .bartxt {font-style: italic}
                    #mainline {stroke:black; fill:none}
                    .markers {fill:black; opacity:1}
                    .title {font-size: 4em; font-weight: bold}
                 """
    # And now the arc colours proper
    c_list = colour_list(len(positions), chroma_var=0, lightness_var=10)
    if opts['jumble_colours']:
        c_list = jumbled(c_list)

    printv("colour list:", c_list)

    colour_by = 't' if opts['swap_colours'] else 'f'
    styles += """\n\n/* Colouration of the arcs: either To or From, and then an index */"""
    for c in range(len(c_list)):
        styles += "\n."+colour_by+str(c)+' {stroke:'+c_list[c]+'}'

    dwg.defs.add(dwg.style(styles)) # and we're done


    # We actually can vary this a little. Consider a nominal 'between' width of 2.5x - we want up to 2.0 of lines plus a 0.5 buffer
    # but it doesn't have to be that either side gets 1.0 each
    # it's actually OK if one side is greater than 1.0, as long as the other is correspondingly less

    tots_max = (tots_max_ds + tots_max_og) / ((between - min_gap) / space)
    # as long as the divisor > 1, will result in wider lines than if we just took max(tmds, tmog)
    # and divisor formulation forces min_gap to exist

    # Destination [sub]totals setup, used for arc positioning
    subtotal_dests = {k:0 for k in positions}

    current_load = 0 # for bargraph
    for i in range(len(positions)): # this rather than tree iteration for order reasons
        orig = positions[i]
        orig_name = stop_names.get(orig, orig)
        if (not orig in tree): # no boardings!
            #printv(orig, "not in tree")
            pass
        else:
            total = space * total_ogs.get(orig, 0) / tots_max  #
            subtotal_og = 0
            for j in range(i+1, len(positions)): # start at i+1 - the next stop along
                dest = positions[j]
                #dest_name = stop_names.get(orig, f"Stop ID {dest}")
                if (not dest in tree[orig]):
                    #printv(dest, f"not in tree[{orig}]")
                    pass
                else:
                    qty = tree[orig][dest]
                    #alt_txt = f"{orig_name} to {dest_name}: {qty}" # at least label with str(qty)
                    destpos = positions.index(dest)
                    width = space * qty / tots_max
                    x1 = round( (left_extra + i*between) + (total - (subtotal_og + width/2)) + space/50       , 5)
                    y1 = main_height
                    x2 = round( (left_extra + destpos*between) + -(width/2 + subtotal_dests[dest]) + -space/50  , 5)
                    y2 = y1
                    draw = "m{} {} {} {} A1 1 0 1 1 {} {} v{}".format(x1, docheight, 0, -text_section, x2, y2, text_section)
                    path = dwg.path(d=draw, stroke_width=round(width,5), class_=f"arc f{i} t{destpos}")
                    #                       ^^^ stroke=c_list[i], fill="none" now in stylesheet.
                    # Note the 'from' and 'to' info tucked away in the path classes. Could use this e.g. on hover
                    dwg.add(path)

                    subtotal_og += width
                    subtotal_dests[dest] += width


        # Also have a total load bargraph
        # No need for a flag - can go `display:none` in CSS if desired
        current_load = current_load + total_ogs.get(orig, 0) - total_dests.get(orig, 0)
        if current_load > 0:
            #printv("Load at "+orig+": ", current_load)
            b_x = left_extra + i*between + between/2
            b_y0 = docheight
            b_y1 = docheight - ((space*current_load)/tots_max)
            dwg.add(dwg.line((b_x, b_y0), (b_x, b_y1), stroke_width=between, class_="bargraph"))
            dwg.add(dwg.text(str(current_load), (b_x, b_y0-space/5), text_anchor="middle", class_="keyline bartxt"))
            # once for the bg and once for the fg
            dwg.add(dwg.text(str(current_load), (b_x, b_y0-space/5), text_anchor="middle", class_="foreground bartxt"))

        # once that's done, time for path/stop labels
        # The easiest way to get a keyline is just to do everything twice.
        t_x = left_extra+(i*between) - space/8
        t_y = main_height+space/2

        line2 = "{} alightings | {} boardings".format(total_dests.get(orig, 0), total_ogs.get(orig, 0))
        textbasebg = dwg.text(orig_name, (t_x, t_y), text_anchor="end", transform="rotate(270,{},{})".format(t_x, t_y),
                              font_size=space/2, class_="keyline")
        textbasebg.add(dwg.tspan(line2, (t_x, t_y+space/2)))
        textbasefg = dwg.text(orig_name, (t_x, t_y), text_anchor="end", transform="rotate(270,{},{})".format(t_x, t_y),
                              font_size=space/2, class_="foreground")
        textbasefg.add(dwg.tspan(line2, (t_x, t_y+space/2)))
        dwg.add(textbasebg)
        dwg.add(textbasefg)

    # end the big iteration


    # Finally add a path with markers

    # first create the marker object. A red circle will do.
    #marker = dwg.marker()
    #marker.add(dwg.circle(r=10, fill="green"))
    #dwg.defs.add(marker)

    # Now create the main line
    line = dwg.add(dwg.polyline(points=[(left_extra+(i*between), main_height) for i in range(len(positions))], id_="mainline"))
    #line['marker'] = marker.get_funciri()

    # since markers aren't working for us, we'll have to go DIY...
    for i in range(len(positions)):
        dwg.add(dwg.circle((left_extra+(i*between), main_height), r=space/4, class_="markers"))


    # add a title if desired
    t_month = ""
    t_year = ""
    try:
        t_year, t_month = r[pfh.index(opts["INFILE_HEADERS"]["month"])].split('-')
        t_month = {"01":"January","02":"February","03":"March","04":"April","05":"May", "06":"June","07":"July","08":"August","09":"September","10":"October","11":"November","12":"December"}[t_month]
    except Exception:
        pass # this isn't really critical

    title_text = f"{routename} {routedirection} – {t_month} {t_year}"
    dwg.add(dwg.text(title_text, (docwidth/2, space*2), class_="title", text_anchor="middle"))

    # and save!
    dwg.save()



if __name__ == "__main__":

    # Set up argument parser
    parser = argparse.ArgumentParser(description="Make line-flow diagrams from origin-destination patronage data, interpreting GTFS for metadata. Requires xsv.", epilog="Why 'fluvial? Because transit is like a river. xsv can be found at https://github.com/BurntSushi/xsv")
    p_v = parser.add_mutually_exclusive_group()
    p_v.add_argument('-v', '--verbose', action="store_true", help="I want to know more")
    p_v.add_argument('--quiet', action="store_true", dest="no_verbose", help="Override definitions file.")
    p_r = parser.add_mutually_exclusive_group()
    p_r.add_argument('-r', '--reflexive', action="store_true", help="include data rows where the origin matches the destination")
    p_r.add_argument('--no-reflexive', action="store_true", help="Override definitions file.")
    p_s = parser.add_mutually_exclusive_group()
    p_s.add_argument('-s', '--swap-colours', action="store_true", help="colour by destination instead of by origin")
    p_s.add_argument('--no-swap-colours', action="store_true", help="Override definitions file.")
    p_j = parser.add_mutually_exclusive_group()
    p_j.add_argument('-j', '--jumble-colours', action="store_true", help="colour neighbours differently rather than similarly")
    p_j.add_argument('--no-jumble-colours', action="store_true", help="Override definitions file.")
    parser.add_argument('-c', '--css', type=argparse.FileType('r'), help="Embed a CSS file in the SVG[s]")
    parser.add_argument('-d', '--definitions', type=argparse.FileType('r'), help="Use a definitions file (overridable by flags)")
    parser.add_argument('-o', '--one', nargs=2, metavar=("ROUTE","DIRECTION"), help="Generate visualisation for only one route/direction combination.")
    p_gp = parser.add_mutually_exclusive_group()
    p_gp.add_argument('-g', '--gtfs_dir', help="Determine stop names and sequences from a folder of GTFS files")
    p_gp.add_argument('-p', '--positions_file', type=argparse.FileType('r'), help="Define a route's stop sequences manually rather than using GTFS (requires -o and is required if not using -d)")
    parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), help="Patronage Data file.")
    parser.add_argument('outdir', nargs='?', help="Where to put the resultant SVG[s], named `{route}_{direction}.svg`. If unspecified, defaults to the current working directory.")

    oc = parser.parse_args() # oc for "_o_ptions, _c_ommand-line interface"


    # Deal with options galore #

    opts = {} # dict to hold all options and definitions after merging flags/definitions/defaults

    # Load up
    cfgp = configparser.ConfigParser()
    if oc.definitions:
        cfgp.read(oc.definitions.name)

    cfgd = {i[0] : {} for i in cfgp.items()} # dict of cfg, holding section : [option]

    for h in ["FLAGS", "PATHS", "INFILE_HEADERS", "INFILE_FILTERS", "DIRECTIONS"]:
        if h in cfgd:
            cfgd[h] = {i[0]:i[1] for i in cfgp.items(h)}
        else:
            print(f'Bad definitions file: missing section "{h}"', file=sys.stderr)
            exit(1)

    # The next few lines are just slogging our way through the flags > defs file > default behaviour

    voc = vars(oc)

    # flags
    for i in ["verbose", "reflexive", "swap_colours", "jumble_colours"]:
        opts[i] = voc.get(i) if (voc.get(i) or voc.get("no_"+i)) else cfgp.getboolean('FLAGS', i, fallback=False)

    # paths
    for i in ["css", "gtfs_dir", "positions_file", "infile", "outdir"]: # also CLI
        opts[i] = voc.get(i) if voc.get(i) else cfgp.get('PATHS', i, fallback=None)
    for i in ['gtfs_cache']: # not CLI
        opts[i] = cfgp.get('PATHS', i, fallback=None)


    # other sections
    for h in ["INFILE_HEADERS", "INFILE_FILTERS", "DIRECTIONS"]:
        opts[h] = cfgd[h]


    # test that all infile_headers are defined
    if oc.definitions:
        for i in ['route', 'direction', 'origin_stop', 'destination_stop', 'quantity', 'ticket_type']:
            if i not in opts["INFILE_HEADERS"]:
                print(f'Bad definitions file: missing option "{i}" in section "INFILE_HEADERS"', file=sys.stderr)
                exit(1)

    #set verbose now
    __VERBOSE = opts['verbose']

    # Sanitise Files

    # Make a tempdir for the GTFS cache (whether we ended up needing it or not; it will be cleaned up on exit.
    gtfs_cache_tempdir = tempfile.TemporaryDirectory()

    if (not oc.infile):
        if opts['infile'] is not None:
            opts['infile'] = open(cfgp.get('PATHS', 'infile'), 'r')
        else:
            opts['infile'] = sys.stdin
    if (not oc.positions_file) and (opts['positions_file'] is not None):
        opts['positions_file'] = open(cfgp.get('PATHS', 'positions'), 'r')
    if opts['gtfs_cache'] == None:
        opts['gtfs_cache'] = gtfs_cache_tempdir.name
    if opts['outdir'] == None:
        opts['outdir'] = os.path.join(os.getcwd(), "fluvial_output")

    if not os.path.exists(opts['outdir']):
        os.makedirs(opts['outdir'])
        printv("Had to create", opts['outdir'])

    # one vs all we worry about now: are we a one-shot or are we an everything?

    printv("opts", opts)

    if oc.one:
        routename = oc.one[0]
        routedirection = oc.one[1]

        process(opts, routename, routedirection)

    else:
        # we need to iterate over all of the things
        # test for xsv before we go any further
        if not shutil.which('xsv'):
            print("Missing `xsv`; get it at https://github.com/BurntSushi/xsv", file=sys.stderr)
            exit(1)

        routedir = sorted(get_route_direction_pairs(opts['infile'], opts['INFILE_HEADERS']['route'], opts['INFILE_HEADERS']['direction']))
        printv("Launching iteration!")
        for routename, routedirection in routedir:
            #printv(routename, routedirection)
            if opts['DIRECTIONS'][routedirection.lower().replace(' ', '_')] not in ['0', '1']:
                printv(f"Skipping {routename} {routedirection}; invalid direction.")
                continue
            process(opts, routename, routedirection)
