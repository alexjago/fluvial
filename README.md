# Fluvial

[*Because transit is like a river.*](https://humantransit.org/2011/02/basics-branching-or-how-transit-is-like-a-river.html)

A program for generating bus route patronage visualisations from origin-destination data.

Note: this program has been going through a rewrite. It is now (v0.3) much faster but a little less flexible than the previous version (v0.2).
The old code is still in the repository for now. Its documentation is in readme-old.md. 

## Installation

Get a [binary for your platform here.](https://github.com/alexjago/fluvial/releases/latest) 

If you'd like to install from source, first install the Rust toolchain and then run `cargo install --git https://github.com/alexjago/fluvial.git`

## Usage

Be sure to get your patronage and route data ready.

By default Fluvial will seek to generate visualisations for every possible route, which takes a while. Use `-o ROUTE DIRECTION` to generate just one thing at a time for initial testing.

## Patronage data

This tool was developed with and is intended for data from [TransLink SEQ](https://translink.com.au/). 

That data is released here: https://data.qld.gov.au/dataset/go-card-transaction-data

Download and put it somewhere accessible.


## GTFS files

TransLink SEQ's GTFS data can be found here:

https://data.qld.gov.au/dataset/general-transit-feed-specification-gtfs-seq

**Caveat: the patronage data has historically been up to a year behind whereas the GTFS data is always current. Luckily routes don't change often.**

Try [transitfeeds.com](https://transitfeeds.com) if you need GTFS for a specific time (for example, if a route is seasonal).

