# Fluvial

[*Because transit is like a river.*](https://humantransit.org/2011/02/basics-branching-or-how-transit-is-like-a-river.html)

A script for generating bus route patronage visualisations from origin-destination data.

Assumes Python 3.6 or newer.

Requires [`xsv`](https://github.com/BurntSushi/xsv), so that timely CSV manipulations can be performed.


## Usage

First set up the virtual environment for the dependencies and activate it.

At this point you can run `$ python3 fluvial.py -h` for the built-in help.

Be sure to get your patronage and route data ready.

You'll probably want use a definitions file rather than specifying everything on the CLI every time. A default setup is provided in `definitions.ini`.

By default Fluvial will seek to generate visualisations for every possible route, which takes a while. Use `-o ROUTE DIRECTION` to generate just one thing at a time for initial testing.

## Patronage data

This tool was developed with data from [TransLink SEQ](https://translink.com.au/) in mind. That data is sporadically released here:

https://data.qld.gov.au/dataset/go-card-transaction-data

Download and put it somewhere accessible.


## GTFS or position files

TransLink SEQ's GTFS data can be found here:

https://data.qld.gov.au/dataset/general-transit-feed-specification-gtfs-seq

**Caveat: the patronage data is often up to a year behind whereas the GTFS data is always current. Luckily routes don't change often.**

If your origin-destination data CSV has columns for what GTFS calls `stop_id` (as TransLinkSEQ's does) then if you also have relevant GTFS data you can most likely connect the two to automatically infer the order of stops along each route.

Otherwise, you will need to manually produce a *positions file* for each route you'd like to visualise, listing what GTFS would call `stop_id`, `stop_name` and `stop_sequence` (and under those headers).

If you somehow don't do either then Fluvial will output stop orders from the order in which they are read from the file.

Fluvial generates a few intermediary files for caching purposes. You may need to set `gtfs_cache` in the definitions file to be something other than a subdirectory of `gtfs_dir`.

## Getting set up: Virtual Environments and Pip howto

This script, like many Python scripts, relies on a couple of third-party things for functionality.

In order to keep things clean, it's the Done Thing in Python to use *virtual environments*.

### If you're on Linux or Mac OS:

Create a virtual environment with `python3 -m venv envdir`.

Activate it: `source envdir/bin/activate`

Install things in the virtual env: `pip3 install -r requirements.txt`

Good to go: run `python3 fluvial.py -h` for specific usage instructions.

Deactivate when done: `deactivate`
(This won't uninstall anything from the virtual env, don't worry.)

### If you're on Windows:

See here to get started: https://docs.python.org/3/library/venv.html

(Also covers non-Windows systems, of course)

And then run `pip3 install -r requirements.txt` once you have the virtual environment activated.

### If you like to live fast

Go `pip3 install --user -r requirements.txt` and install the dependedencies outside of a virtual environment (but still in your user directory, of course).

## Features on the to-do list

- Mouseover support. If SVG paths supported alt-text this would be very simple, but they don't.
- Better `stop_sequence` aggreggation. Many routes have e.g. school-pickup variations, and some are loops with varying points of service commencement around the loop. The current 'averaging' aggregation can lead to incorrect diagrams. A better approach would be some sort of graph construction.
