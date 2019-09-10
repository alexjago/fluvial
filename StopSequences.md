# Stop Sequence calculation

Sometimes, routes have variations.

We must... deal with them.

GTFS thankfully provides a `stop_sequence` field.
This lets us put individual 'runs' in order, but we need to aggregate across all runs.

The current, interim method is to compute the mean `stop_sequence`.
However, this falls down miserably for large loop services, which may begin runs at various points around the loop.

We ought to be treating this as a directed-graph problem.
Since we'd like to display data linearly, we actually want to treat it as a directed *acyclic* graph problem.
(Indeed while we accept that some services are loops and therefore aren't really DAGs, they should still only have the *one* cycle...)

Since we are aggregating a lot of sequences, this is actually a preferential-voting problem (yay!).

The appropriate method to use appears similar to Ranked Pairs:

1. Aggregate 'ballots' into a pairwise comparison matrix.
2. Sort the comparison matrix by margin of victory
3. Add the edges to the graph in order of victory margin, starting with the strongest...
4. Provided that they do not cause a cycle. If they do, don't add them and skip to the next edge.
5. The winner is the source of the graph and a ranked list of winners is created by iteratively reconstructing the graph without the source.

We have a big breaking change though.

Unlike normal voting, being left off a 'ballot' really does mean 'no comparison' rather than coercing to 'equal last'.

This means that our graph will likely have a much lower edge density. In particular we probably have to deal with differing subsequences.
The usual way to visualise this is to present one subsequence in order, then the other.

                   E - F - G - H \
    A - B - C - D -|              L - M - N
                   I - J - K --- /

Is visualised as either

    A - B - C - D - E - F - G - H • I - J - K - L - M - N

or

    A - B - C - D - I - J - K • E - F - G - H - L - M - N

(There are various arguments for which goes first.)

Some definitions:

**Junction:** a node in the TRDAG having more than one parent or more than one child.

**Subsequence:** A path in the TRDAG of non-junction nodes. A subset directed path of the transitively-reduced DAG; with (a) only its start and/or end nodes connected to the rest of the graph; and (b) with every inner node having only one parent and one child; and (c) with every edge node having at most one parent and at most one child.



Sometimes of course there will be two variants: one will run express, skipping some stops, and the other will not.
This is trivial to accommodate because the express' stops are just a subset. Similarly, route extensions pose no issues as long as there's only the one version.

In conclusion, what we want is a topological ordering where subsequences don't get interlaced.

This is because *any* permutation of `EFGHIJK` forms part of a valid topological sort of the graph mentioned above, provided that `E>F>G>H and I>J>K`. But we don't want those; we only want `EFGHIJK` or `IJKEFGH`.

So from the reduction we need to identify subsequences, think of them as supernodes, construct another graph of supernodes, and then we can use any topological ordering of that. But how to identify subsequences?

OK so take *some* topological ordering. If there's only one, use it directly. If there's more than one, just use the first.

    new supergraph
    new list for subsequences

    for each node in the ordering:
        if zero or one parents: # either out-junction or new subsequence
            if zero or one children:
                if (parent exists) and (parent in a subsequence):
                    add self to parent's subsequence
                else:
                    allocate new subsequence
                    add self to subsequence
                    place subsequence in supergraph
                    connect self to parent in supergraph
            else if two or more children: # junction
                add self to supergraph
                for each parent:
                    if parent is junction:
                        connect self to parent in supergraph
                    if parent in subsequence:
                        connect self to subsequence in supergraph                    
        else if two or more parents: # an in-junction node regardless of outs
            add self to supergraph
            for each parent:
                if parent is junction:
                    connect self to parent in supergraph
                if parent in subsequence:
                    connect self to subsequence in supergraph

Then get the topo ordering of the supergraph (doesn't matter which)
Then expand the subsequences
