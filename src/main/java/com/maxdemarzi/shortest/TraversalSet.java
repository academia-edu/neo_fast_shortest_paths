package com.maxdemarzi.shortest;

import java.util.Collection;

import net.openhft.koloboke.collect.map.LongIntMap;
import net.openhft.koloboke.collect.map.hash.HashLongIntMaps;

public final class TraversalSet extends Traversal {

    private final LongIntMap explored;
    private final Traversal[] traversals;
    private int index;

    public TraversalSet(Collection<Traversal> traversals) {
        super();
        this.explored = HashLongIntMaps.newMutableMap(500);
        this.traversals = traversals.toArray(new Traversal[traversals.size()]);
        this.index = 0;
    }

    public void step() {
        for (int i = 0; i < traversals.length; i++) {
            Traversal t = traversals[(this.index + i) % traversals.length];
            if (!t.isFinished()) {
                t.step();
                this.index = (this.index + i + 1) % traversals.length;
                return;
            }
        }
        //If they're all finished, then we're done
        this.finish();
    }

    protected boolean hasExplored(long nodeId) {
        return explored.containsKey(nodeId);
    }

    protected int getCost(long nodeId) {
        return this.traversals[this.explored.get(nodeId)].getCost(nodeId);
    }

    protected int getPaths(long nodeId) {
        return this.traversals[this.explored.get(nodeId)].getPaths(nodeId);
    }
}
