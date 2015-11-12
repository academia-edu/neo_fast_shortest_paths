package com.maxdemarzi.shortest;

import org.neo4j.kernel.api.cursor.NodeItem;

public abstract class Traversal {

    private boolean finished;

    public Traversal() {
        this.finished = false;
    }

    public static interface NodeCallback {
        /**
         * @param traversal: the traversal that did the exploring (the same callback can be given to multiple traversals)
         * @param node: The node that was explored, can be null if the traversal didnt load that node,
         * in that case it is up to the implementer to load the node from the nodeId
         * @param nodeId: the id of the explored node
         * @param cost: the cost to get to this node from the traversal's starting point, in a Breadth first search this will be the depth
         * @param paths: the number of same-cost paths to this node from the starting nodes
         */
        public void explored(final Traversal traversal, final NodeItem node, final long nodeId, final int cost, int paths);
    }

    public void run() {
        while (!this.isFinished()) {
            this.step();
        }
    }

    public void finish() {
        this.finished = true;
    }

    public boolean isFinished() {
        return this.finished;
    }

    public abstract void step();

    protected abstract boolean hasExplored(long nodeId);
    protected abstract int getCost(long nodeId);
    protected abstract int getPaths(long nodeId);
}
