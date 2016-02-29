
package com.maxdemarzi.shortest;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.neo4j.kernel.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

public final class NodeCache {

    private final LoadingCache<String, Long> emails;
    private final LoadingCache<String, Long> bibliographyEntries;

    private GraphDatabaseService db = null;

    private static NodeCache instance = null;

    public static synchronized NodeCache getInstance(GraphDatabaseService db) {
      if (instance == null) {
        instance = new NodeCache(1_000_000);
      }
      instance.useDatabase(db);
      return instance;
    }

    private static final Long getEmailNodeId(GraphDatabaseService db, String email) throws Exception{
        final Node node = db.findNode(Labels.Email, "email", email);
        if (node != null) {
            return node.getId();
        } else {
            throw new Exception("Email not found");
        }
    }

    private static final Long getBibliographyEntryNodeId(GraphDatabaseService db, String bibId) throws Exception{
        final Node node = db.findNode(Labels.BibliographyEntry, "id", Long.valueOf(bibId));
        if (node != null) {
            return node.getId();
        } else {
            throw new Exception("BibliographyEntry not found");
        }
    }

    private NodeCache(long maxSize) {
        emails = CacheBuilder.newBuilder()
            .maximumSize(maxSize).build(new CacheLoader<String, Long>() {
                public Long load(String email) throws Exception {
                    return getEmailNodeId(db, email);
                }
            });
        bibliographyEntries = CacheBuilder.newBuilder()
            .maximumSize(maxSize)
            .build(new CacheLoader<String, Long>() {
                public Long load(String bibId) throws Exception {
                    return getBibliographyEntryNodeId(db, bibId);
                }
            });
    }

    public void useDatabase(GraphDatabaseService db) {
        this.db = db;
    }

    public final Long getEmailNode(String email) throws ExecutionException {
        return emails.get(email);
    }

    public final List<Long> getEmailNodes(Collection<String> emailSet) {
        List<Long> nodeIds = new ArrayList<>(emailSet.size());
        for (String email : emailSet) {
            try {
                nodeIds.add(getEmailNode(email));
            } catch (ExecutionException e) {
                continue;
            }
        }
        return nodeIds;
    }

    public final Long getBibliographEntryNode(String bibliographyEntryId) throws ExecutionException {
        return bibliographyEntries.get(bibliographyEntryId);
    }

    public final List<Long> getBibliographEntryNodes(Collection<String> bibEntries) {
        List<Long> nodeIds = new ArrayList<>(bibEntries.size() + 1);
        for (String bibId : bibEntries) {
            try {
                nodeIds.add(getBibliographEntryNode(bibId));
            } catch (ExecutionException e) {
                continue;
            }
        }
        return nodeIds;
    }
}
