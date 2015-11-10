package com.maxdemarzi.shortest;

import org.apache.commons.lang.NotImplementedException;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;

import net.openhft.koloboke.collect.map.IntIntMap;

/**
 * A specialized traversal that just reports the nodeIds seen at the end of a single node's relationships
 */
public class OneDegreeTraversal extends Traversal {
    private final ReadOperations readOps;
    private final IntIntMap relationshipCosts;
    private final NodeCallback callback;
    private final long startNode;
    private final int startCost;

    public OneDegreeTraversal(ReadOperations readOps, IntIntMap relationshipCosts, long startNode, int startCost, NodeCallback callback) {
        this.readOps = readOps;
        this.relationshipCosts = relationshipCosts;
        this.callback = callback;
        this.startNode = startNode;
        this.startCost = startCost;
    }

    public void step() {
        final Cursor<NodeItem> nodeCursor = this.readOps.nodeCursor(this.startNode);
        nodeCursor.next();
        final NodeItem currentNode = nodeCursor.get();
        final Cursor<RelationshipItem> relationshipCursor = currentNode.relationships(Direction.BOTH);

        while(relationshipCursor.next()) {
            RelationshipItem relation = relationshipCursor.get();
            final int stepCost = this.relationshipCosts.get(relation.type()) + this.startCost;
            this.callback.explored(this, null, relation.otherNode(this.startNode), stepCost, 1);
        }
        this.finish();
    }

    protected boolean hasExplored(long nodeId) {
        throw new NotImplementedException();
    }

    protected int getCost(long nodeId) {
        throw new NotImplementedException();
    }

    protected int getPaths(long nodeId) {
        throw new NotImplementedException();
    }
}
