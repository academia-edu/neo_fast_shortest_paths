package com.maxdemarzi.shortest;

import java.util.Collection;
import java.util.HashMap;
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
import net.openhft.koloboke.collect.map.hash.HashIntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps;
import net.openhft.koloboke.collect.map.hash.HashLongIntMap;
import net.openhft.koloboke.collect.map.hash.HashLongLongMap;
import net.openhft.koloboke.collect.map.hash.HashLongLongMaps;
import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import net.openhft.koloboke.collect.set.hash.HashLongSet;
import net.openhft.koloboke.collect.set.hash.HashLongSets;
import net.openhft.koloboke.function.Consumer;
import net.openhft.koloboke.function.IntIntConsumer;

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
    private final HashLongSet explored;

    /*
     * To avoid having to store a map of NodeId -> Cost + Paths
     * where the value is an object, we compress the cost and paths
     * into a long. Path count is dependent on the distance
     * Probably also faster not to follow an object pointer
     */

    private static final long costPaths(int cost, int paths) {
        return (((long)cost) << 32) | (paths & 0xffffffffL);
    }

    private static final int cost(long costPaths) {
        return (int)(costPaths >> 32);
    }

    private static final int paths(long costPaths) {
        return (int) costPaths;
    }

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
        this.explored = HashLongSets.newMutableSet(500);
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
        // Because we cant modify the priority of things on the queue, we just have to handle
        // encountering duplicates. removing and re-inserting would be super slow
        if (this.explored.contains(current.nodeId)) {
            return;
        }

        final int paths = paths(this.paths.get(current.nodeId));
        final int cost = current.cost;

        final Cursor<NodeItem> nodeCursor = this.readOps.nodeCursor(current.nodeId);
        nodeCursor.next();
        final NodeItem currentNode = nodeCursor.get();
        final Cursor<RelationshipItem> relationshipCursor = currentNode.relationships(Direction.BOTH);

        while(relationshipCursor.next()) {
            RelationshipItem relation = relationshipCursor.get();
            final int stepCost = this.relationshipCosts.get(relation.type()) + cost;
            if (stepCost > this.maxCost) {
                continue;
            }
            final long otherId = relation.otherNode(current.nodeId);
            if (this.explored.contains(otherId)) {
                continue;
            }

            long costPaths = this.paths.get(otherId);
            if (costPaths == 0L) { //equivalent to getting null. this is a new node
                this.paths.put(otherId, costPaths(stepCost, paths));
            } else {
                final int otherCost = cost(costPaths);
                if (stepCost > otherCost) {
                    continue; //we already saw a cheaper way of getting to this node
                } else if (stepCost < otherCost) {
                    //this way is cheaper than an existing one
                    this.paths.put(otherId, costPaths(stepCost, paths));
                } else {
                    //we found a matching-cost path, add paths from this route
                    this.paths.put(otherId, costPaths(stepCost, paths + paths(costPaths)));
                    continue;
                }
            }
            this.queue.offer(new Step(otherId, stepCost));
        }

        this.nodeCallback.explored(this, currentNode, current.nodeId, cost, paths);
        this.paths.remove(current.nodeId);
        this.explored.add(current.nodeId);
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


