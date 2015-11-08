package com.maxdemarzi.shortest;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Map;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;

import net.openhft.koloboke.collect.map.IntIntMap;
import net.openhft.koloboke.collect.map.IntIntMapFactory;
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps;
import net.openhft.koloboke.collect.map.hash.HashLongLongMap;
import net.openhft.koloboke.collect.map.hash.HashLongLongMaps;
import net.openhft.koloboke.function.LongBinaryOperator;

public final class Dijkstra {

    private static final IntIntMapFactory costMapFactory = HashIntIntMaps.
        getDefaultFactory().
        withDefaultValue(3);

    private final int maxCost;
    private final ReadOperations readOps;
    private final NodeCallback nodeCallback;
    private final IntIntMap relationshipCosts;
    private boolean finished;

    private final PriorityQueue<Step> queue;
    private final HashLongLongMap paths;

    /*
     * To avoid having to store a map of NodeId -> Cost + Paths + Explored
     * where the value is an object, we compress the fields into a long
     * The explored boolean takes the sign bit of the cost int.
     *
     * Probably also faster not to follow an object pointer and better cache
     * behavior to have a small footprint
     */
    private static final long exploredMask = 0x80000000L;

    private static final long costPaths(int cost, int paths) {
        return (((long)paths) << 32) | (cost & 0xffffffffL);
    }

    private static final long setExplored(long costPaths) {
        return exploredMask | costPaths;
    }

    private static final int cost(long costPaths) {
        return (int) (costPaths & ~exploredMask);
    }

    private static final int paths(long costPaths) {
        return (int)(costPaths >> 32);
    }

    private static final boolean explored(long costPaths) {
        return (exploredMask & costPaths) > 0;
    }

    private static final long addPaths(long a, long b) {
        return (0xffffffff00000000L & a) + b;
    }

    /*
     * This function is called for every relationship we follow to update the cost & path count to a node
     * It happens within the koloboke HashLongLongMap merge function, this way it avoids an additional hash lookup
     */
    private static final LongBinaryOperator updateSeenFunc = new LongBinaryOperator() {
            public long applyAsLong(long oldVal, long newVal) {
                if (explored(oldVal)) {
                    return oldVal;
                }
                final int oldCost = cost(oldVal);
                final int newCost = cost(newVal);
                if (newCost < oldCost) {
                    return newVal;
                } else if (newCost > oldCost) {
                    return oldVal;
                } else {
                    return addPaths(oldVal, newVal);
                }
            }
        };

    public static interface NodeCallback {
        public void explored(final Dijkstra traveral, final NodeItem node, final long nodeId, final int cost, int paths);
    }

    private static final class Step implements Comparable<Step> {
        public final long nodeId;
        public final int cost;

        public Step(long nodeId, int cost) {
            this.nodeId = nodeId;
            this.cost = cost;
        }

        public int compareTo(Step o) {
            if (this.cost < o.cost) {
                return -1;
            } else if (this.cost > o.cost) {
                return 1;
            }
            return 0;
        }
    }

    public Dijkstra(ReadOperations readOps, Map<String, Integer> relationshipCosts, Collection<Long> startNodes, int maxCost, NodeCallback callback) {
        // Convert Relationship Names to IDs
        this.relationshipCosts = costMapFactory.newMutableMap(relationshipCosts.size());
        for (Entry<String, Integer> entry : relationshipCosts.entrySet()) {
            this.relationshipCosts.put(readOps.relationshipTypeGetForName(entry.getKey()), entry.getValue().intValue());
        }

        this.readOps = readOps;
        this.nodeCallback = callback;
        this.queue = new PriorityQueue<Step>();
        this.paths = HashLongLongMaps.newMutableMap(500);
        this.maxCost = maxCost;
        this.finished = false;
        //initialize the queue with our start nodes
        for (Long nodeId : startNodes) {
            Step step = new Step(nodeId.longValue(), 0);
            this.paths.put(nodeId.longValue(), costPaths(0, 1));
            this.queue.offer(step);
        }
    }

    public void step() {
        final Step current = this.queue.poll();
        if (current == null) {
            this.finished = true;
            return;
        }
        final long exploredCostPaths = this.paths.get(current.nodeId);
        // Because we cant modify the priority of things on the queue, we just have to handle
        // encountering duplicates. removing and re-inserting would be super slow
        if (explored(exploredCostPaths)) {
            return;
        }

        final int paths = paths(exploredCostPaths);
        final int cost = cost(exploredCostPaths);

        final Cursor<NodeItem> nodeCursor = this.readOps.nodeCursor(current.nodeId);
        nodeCursor.next();
        final NodeItem currentNode = nodeCursor.get();
        final Cursor<RelationshipItem> relationshipCursor = currentNode.relationships(Direction.BOTH);
        int degree = 0;

        if (cost != this.maxCost) { // if we're at max cost, dont bother looking at edges
            while(relationshipCursor.next()) {
                degree++;
                RelationshipItem relation = relationshipCursor.get();
                final int stepCost = this.relationshipCosts.get(relation.type()) + cost;
                if (stepCost > this.maxCost) {
                    continue;
                }
                final long otherId = relation.otherNode(current.nodeId);
                final long newVal = costPaths(stepCost, paths);

                final long result = this.paths.merge(otherId, newVal, updateSeenFunc);
                if (result == newVal) {
                    this.queue.offer(new Step(otherId, stepCost));
                }
            }
        }
        this.nodeCallback.explored(this, currentNode, current.nodeId, cost, paths);
        // If we explored the relationships on this node and it's degree was 1,
        // then it is safe to forget this node in our paths tracking, because we wont encounter it again
        // from this or any other traversal that doesnt also intersect with a lower cost path. (its a dead end)
        // should save a bit of memory
        if (degree == 1) {
            this.paths.remove(current.nodeId);
        } else {
            this.paths.put(current.nodeId, setExplored(exploredCostPaths));
        }
    }

    public void run() {
        while (!this.isFinished()) {
            this.step();
        }
    }

    public boolean isFinished() {
        return this.finished;
    }

    public void finish() {
        this.finished = true;
    }
}


