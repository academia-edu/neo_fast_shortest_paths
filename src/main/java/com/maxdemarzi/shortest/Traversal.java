package com.maxdemarzi.shortest;

import org.neo4j.kernel.api.cursor.NodeItem;

public abstract class Traversal {

    private boolean finished;

    public Traversal() {
        this.finished = false;
    }

    public static interface NodeCallback {
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
