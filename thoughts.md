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

In further support of these goals there should be a way to mark a stop as boardings-only or alightings-only. This avoids counting e.g. Bowen Hills boardings as part of inbound Ferny Grove line patronage.

So we have the following columns: 

* stop id (corresponds to origin_stop or destination_stop)
* stop name
* stop sequence number (not necessarily unique)
* nominal route (corresponds to route)
* nominal direction (corresponds to direction)
* actual route
* actual direction
* Stop usage: board-only=1;  alight-only=2; both=0