#!/usr/bin/python3

import csv
import subprocess
import argparse
import sys
from urllib.request import urlopen
from zipfile import ZipFile
import tempfile
from io import BytesIO
import os


if sys.version_info.major == 3 and sys.version_info.minor < 5:
    print("Sorry, this script needs a newer Python.", file=sys.stderr)
    exit(1)

parser = argparse.ArgumentParser(description='Run Fluvial for all available months.', allow_abbrev=False, epilog="any unrecognised flags will be passed on")

parser.add_argument('--runfile', type=argparse.FileType('r'), default=sys.stdin, help="The run CSV: [Patronage ZIP URL, GTFS ZIP URL]")
parser.add_argument('--fluvial', help="path to the Fluvial binary", default="fluvial")
parser.add_argument("--outdir", help="output directory", default=os.getcwd())

(argus, argrest) = parser.parse_known_args()


with argus.runfile as csvfile:
    rdr = csv.reader(csvfile, dialect="excel")
    for row in rdr:
        patr_url = row[0]
        gtfs_url = row[1]

        # Excel puts a byte order mark \ufeff at the start of the file.
        # urlopen hates this. Find out what's going on with this line if needed
        ##print(repr(patr_url))

        with tempfile.TemporaryDirectory() as td:

            # set up patronage file
            patr_path = None

            print("downloading and extracting", patr_url)

            with urlopen(patr_url) as zipresp:
                with ZipFile(BytesIO(zipresp.read())) as zfile:
                    for fn in zfile.namelist():
                        if fn.endswith(".csv"):
                            patr_path = zfile.extract(fn, path=td)

            # set up GTFS dir


            print("downloading and extracting", gtfs_url)

            with urlopen(gtfs_url) as zipresp:
                with ZipFile(BytesIO(zipresp.read())) as zfile:
                    yes = []
                    for fn in zfile.namelist():
                        if fn.endswith(".txt"):
                            yes.append(fn)
                    zfile.extractall(path=td, members=yes)

            pg_args = [argus.fluvial] + [i for i in argrest] + ['-g', td, patr_path, argus.outdir]

            print(pg_args)

            subprocess.run(pg_args)
