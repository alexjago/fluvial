#!/usr/bin/env python3

import argparse
import subprocess
import sys
import glob
from os.path import abspath, join

parser = argparse.ArgumentParser(description="Construct a time-series animation from a sequence of diagrams for a specific route.", epilog="This program largely wraps svgasm; the -d, -e, -i and -o options here simply map to options there.")
parser.add_argument("-d", "--delaysecs", help="animation time delay in seconds  (default: 0.1)")
parser.add_argument("-e", "--endframe", help=" index of frame to stop at in last iteration if not infinite  (default: -1)")
parser.add_argument("-i", "--itercount", help="index of frame to stop at in last iteration if not infinite  (default: -1)")
parser.add_argument("-o", "--outfile", help="path to SVG animation output file or - for stdout  (default: -)")
parser.add_argument("--ftime", help="use diagrams that were filtered by this time column in the patronage file (default: N/A)")
parser.add_argument("-y", "--year", help="specify a single year to use, rather than all of them")
parser.add_argument("route_name", help="the route's short name or number")
parser.add_argument("direction", help="the route's direction")
parser.add_argument("source_dir", help="fluvial's *output* directory")

opts = parser.parse_args()

svgasm = subprocess.run(["which", 'svgasm'],
                stdout=subprocess.PIPE,
                universal_newlines=True)\
                .stdout.strip()
                
if not svgasm:
    print("Error: could not find svgasm to perform animations with", file=sys.stderr)
    exit(1)

##### Configure search path #####

searchpath = abspath(opts.source_dir)

## search for years
if opts.year:
    searchpath = join(searchpath, opts.year)
else:
    searchpath = join(searchpath, "[0123456789]"*4)
    
## and for months
searchpath = join(searchpath, "[0123456789]"*2)

## and time, if relevant
if opts.ftime:
    searchpath = join(searchpath, opts.ftime)
    
## and finally specify the correct SVG
searchpath = join(searchpath, f"{opts.route_name}_{opts.direction}.svg")

paths = sorted(glob.glob(searchpath))

if not paths:
    print("Error: could not find any diagrams matching this specification", file=sys.stderr)
    exit(1)


##### Construct the arguments #####

args = [svgasm, '-c', 'cat "%s"', '-q']

if opts.delaysecs:
    args += ["-d", opts.delaysecs]
    
if opts.endframe:
    args += ["-e", opts.endframe]
    
if opts.itercount:
    args += ["-i", opts.itercount]
    
if opts.outfile:
    args += ["-o", opts.outfile]

args += paths

# run the subprocess!

subprocess.run(args)