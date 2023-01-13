# Thoughts

## The Positions File

The positions file's main purpose is to permit line maps for Rail, GCLR and Busway trips. 
The problem with these is that the `route` and `direction` names in the patronage data are a bit useless, lumping together any and all lines, and both directions.

So we need a way to supply our own name and direction, and a stop sequencing too. 

An additional problem with e.g. Rail in particular is that people can change trains without tapping off. There needs to be a way to categorise these trips. 

The main issue is that two directions have the same code and need to be distentangled. 

For non-loops you can assign flows to one way (or the other) by origin > destination, for one-way loops you just wrap around, but two-way loops might be a bit too much; the only hope there might be that the station codes differ.  

We'd also like to support parent station clumping. This is probably representable by just giving them the same name and stop-sequence number. 

Supporting station clumping also lets us handle journeys to/from other lines, albeit in a clumsy way - just assign all stop_ids to the same stop_sequence number and name it "to|from XYZ Line". 

We also need to deal with parallel or express lines. For example, the Gold Coast line is an express overlay on the Beenleigh line. Altandi, Loganlea and Beenleigh stations are the express stops and we should expect passengers to preferentially catch the train between those stations on the express service. Alternatively, the Ipswich and Springfield lines presently run alternating-combined from Darra to Northgate; we expect passengers between stations there to catch whatever shows up first. Similarly on the CBD lines.

We can deal with the parallel-lines case by allocating a station _weighting_ and then passenger trips between stations with a reduced weighting account for parallel services.
We take the _higher_ weighting of the pair of stations. Examples:

* Darra & Graceville both have a weighting of 0.5 for the Ipswich & Springfield lines; trips between those stations are split half-and-half between the lines. However, Bundamba is only on the Ipswich line so it is weighted 1. Trips between Bundamba & Darra are weighted at 1.
* Loganlea, Altandi and Beenleigh are on both the Gold Coast and Beenleigh lines with the Gold Coast as an express overlay. These stations are weighted 0 for the Beenleigh line but 1 for the Gold Coast line. Other Beenleigh-only stations are weighted 1, so trips from those to an express station are counted at full value for the Beenleigh line, but trips between the express stations are counted at full value for the Gold Coast line.
* Trips which are downweighted to zero value should probably be _depicted_ with some sort of dashed line and text ("Up to X trips; all allocated to another line)
* Similarly trips which are downweighted to a non-zero value should have text noting the total number of trips and the weighting. 

CAVEAT: zero-for-express-overlay doesn't really work once you consider combined stations. Consider trips from Loganlea to Central. Since Central presumably has a partial weighting on the Beenleigh line, those trips are counted at that partial weighting even when Loganlea is zero-weighted. Also, the concept of allocating people exclusively to the expresses is questionable. Perhaps higher than half-and-half, but not all-and-nothing.

CAVEAT #2: referring to combined sections of other lines as a pseudo-entry. Workaround: consider the combined section separately.

So we have the following columns:

* stop id (corresponds to origin_stop or destination_stop)
* stop name
* stop sequence number (not necessarily unique)
* nominal route (corresponds to route)
* nominal direction (corresponds to direction)
* actual route
* actual direction
* weighting

We could also potentially have stop IDs be the trailing field (in a CSV) and have arbitrarily many of them:

* Display route name
* Display route direction
* Display name
* Lookup route name
* Lookup route direction
* Stop sequence (NOT unique)
* Weighting
* Stop IDs (one or more columns)

OK yes you could, but then you need to figure out a way to deal with the variadic last column rather than just importing the thing into SQLite.

So, just have a single stop ID, duplicate stop sequencing (sum grouped by everything but stop id itself) and then we have something that fits the shape of a database table

