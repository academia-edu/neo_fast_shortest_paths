package com.maxdemarzi.shortest;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import net.openhft.koloboke.collect.LongCursor;
import net.openhft.koloboke.collect.map.LongObjCursor;
import net.openhft.koloboke.collect.map.IntIntMap;
import net.openhft.koloboke.collect.map.LongIntCursor;
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps;
import net.openhft.koloboke.collect.map.hash.HashLongIntMap;
import net.openhft.koloboke.collect.map.hash.HashLongIntMaps;
import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import net.openhft.koloboke.collect.set.LongSet;
import net.openhft.koloboke.collect.set.hash.HashLongSets;

import org.apache.commons.lang3.mutable.MutableInt;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.lang.Iterable;

import static com.maxdemarzi.shortest.Validators.getValidQueryInput;
import static com.maxdemarzi.shortest.Validators.getValidDijkstraInput;

@Path("/service")
public class Service {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static GraphDatabaseService db;
    private final GraphDatabaseAPI dbAPI;
    private final NodeCache nodeCache;

    public Service(@Context GraphDatabaseService graphDatabaseService) {
        db = graphDatabaseService;
        dbAPI = (GraphDatabaseAPI) db;
        nodeCache = NodeCache.getInstance(db);
    }

    @GET
    @Path("/helloworld")
    public Response helloWorld() throws IOException {
        Map<String, String> results = new HashMap<String,String>(){{
            put("hello","world");
        }};
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    /**
     * JSON formatted body requires:
     *  center_email: An email address
     *  edge_emails: An Array of email addresses
     *  length: An integer representing the maximum traversal search length
     */
    @POST
    @Path("/query")
    public Response query(String body, @Context GraphDatabaseService db) throws IOException, ExecutionException {
        ArrayList<HashMap> results = new ArrayList<>();
        ArrayList<Node> edgeEmailNodes = new ArrayList<>();

        // Validate our input or exit right away
        HashMap input = getValidQueryInput(body);

        try (Transaction tx = db.beginTx()) {
            final Node centerNode;
            try {
                centerNode = db.getNodeById(nodeCache.getEmailNode((String) input.get("center_email")));
            } catch (ExecutionException e) {
                return Response.ok().entity("[]").build();
            }

            for (String edgeEmail : (ArrayList<String>)input.get("edge_emails")) {
                Long edgeId;
                try {
                    edgeId = nodeCache.getEmailNode(edgeEmail);
                } catch (Exception e) {
                    continue;
                }
                final Node edgeEmailNode = db.getNodeById(edgeId);
                edgeEmailNodes.add(edgeEmailNode);
            }

            PathExpander<?> expander =  PathExpanders.allTypesAndDirections();
            PathFinder<org.neo4j.graphdb.Path> shortestPath = GraphAlgoFactory.shortestPath(expander, (int)input.get("length"));

            for (Node edgeEmail : edgeEmailNodes ) {
                HashMap<String, Object> result = new HashMap<>();
                int length = 0;
                int count = 0;
                for ( org.neo4j.graphdb.Path path : shortestPath.findAllPaths( centerNode, edgeEmail ) )
                {
                    length = path.length();
                    count++;
                }

                if (length > 0 && count > 0) {
                  result.put("email", edgeEmail.getProperty("email", ""));
                  result.put("length", length);
                  result.put("count", count);

                  results.add(result);
                }
            }
        }
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    /**
     * JSON formatted body requires:
     *  center_email: An email address
     *  edge_emails: An Array of email addresses
     *  length: An integer representing the maximum traversal search length
     */
    @POST
    @Path("/query_streaming")
    public Response query_streaming(String body, @Context GraphDatabaseService db) throws IOException, ExecutionException {

        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                // Validate our input or exit right away
                HashMap input = getValidQueryInput(body);

                String centerEmail = (String) input.get("center_email");
                List<String> edgeEmails = (ArrayList<String>) input.get("edge_emails");
                List<String> bibEntries = (ArrayList<String>) input.get("bibliography_entries");
                int length = (int) input.get("length");

                streamShortestPathsUsingBuiltinAlgo(centerEmail, bibEntries, edgeEmails, length, jg);

                jg.close();
            }
        };
        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

    /**
     * JSON formatted body requires:
     *  center_email: An email address
     *  edge_emails: An Array of email addresses
     *  length: An integer representing the maximum traversal search length
     */
    @POST
    @Path("/query_counters")
    public Response query_counters(String body, @Context GraphDatabaseService db) throws IOException, ExecutionException {

        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                // Validate our input or exit right away
                HashMap input = getValidQueryInput(body);

                String centerEmail = (String) input.get("center_email");
                List<String> edgeEmails = (ArrayList<String>) input.get("edge_emails");
                List<String> bibEntries = (ArrayList<String>) input.get("bibliography_entries");
                int length = (int) input.get("length");

                streamShortestPathsUsingHandwrittenBFS(centerEmail, bibEntries, edgeEmails, length, jg);

                jg.close();
            }
        };
        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

    /**
     * JSON formatted body requires:
     *  center_email: An email address
     *  edge_emails: An Array of email addresses
     *  length: An integer representing the maximum traversal search length
     */
    @POST
    @Path("/query_either")
    public Response query_either(String body, @Context GraphDatabaseService db) throws IOException, ExecutionException {

        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                // Validate our input or exit right away
                HashMap input = getValidQueryInput(body);

                String centerEmail = (String) input.get("center_email");
                List<String> bibEntries = (ArrayList<String>) input.get("bibliography_entries");
                List<String> edgeEmails = (ArrayList<String>) input.get("edge_emails");
                int length = (int) input.get("length");

                if (edgeEmails.size() <= length) {
                    // There are few target nodes, so search using the built-in algorithm which (presumably) does a BFS from each end for each
                    streamShortestPathsUsingBuiltinAlgo(centerEmail, bibEntries, edgeEmails, length, jg);
                } else {
                    // There are many target nodes, so search using a BFS only from the source node
                    streamShortestPathsUsingHandwrittenBFS(centerEmail, bibEntries, edgeEmails, length, jg);
                }

                jg.close();
            }
        };
        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

    @POST
    @Path("/query_shortest")
    public Response query_shortest(String body, @Context GraphDatabaseService db) throws IOException, ExecutionException {

        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                HashMap input = getValidDijkstraInput(body);

                String centerEmail = (String) input.get("center_email");
                List<String> bibEntries = (List<String>) input.get("bibliography_entries");
                List<String> edgeEmails = (List<String>) input.get("edge_emails");
                int maxCost = (int) input.get("max_cost");
                Map<String,Integer> edgeCosts = (Map<String,Integer>) input.get("edge_costs");

                streamShortestPathsUsingDijkstra(centerEmail, bibEntries, edgeEmails, maxCost, edgeCosts, jg);
                jg.close();
            }
        };
        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

    private static final List<Node> nodesById(Iterable<Long> ids) {
        List<Node> nodes = new ArrayList<>();
        for (Long id : ids) {
            nodes.add(db.getNodeById(id));
        }
        return nodes;
    }

    private void streamShortestPathsUsingBuiltinAlgo(String centerEmail, List<String> bibEntries, List<String> edgeEmails, int maxLength, JsonGenerator jg) throws IOException {

        try (Transaction tx = db.beginTx()) {
            final Node centerNode;
            try {
                centerNode = db.getNodeById(nodeCache.getEmailNode(centerEmail));
            } catch (ExecutionException e) {
                return;
            }

            final Collection<Node> edgeEmailNodes = nodesById(nodeCache.getEmailNodes(edgeEmails));
            final Collection<Node> bibEntryNodes = nodesById(nodeCache.getBibliographEntryNodes(bibEntries));

            PathExpander<?> expander = PathExpanders.allTypesAndDirections();
            PathFinder<org.neo4j.graphdb.Path> shortestPath = GraphAlgoFactory.shortestPath(expander, maxLength);
            PathFinder<org.neo4j.graphdb.Path> shortestPathViaBib = GraphAlgoFactory.shortestPath(expander, maxLength - 1);

            for (Node edgeEmail : edgeEmailNodes) {
                HashMap<String, Object> result = new HashMap<>();
                int length = 0;
                int count = 0;
                for (org.neo4j.graphdb.Path path : shortestPath.findAllPaths(centerNode, edgeEmail)) {
                    length = path.length();
                    count++;
                }
                for (Node bibEntry : bibEntryNodes) {
                    for (org.neo4j.graphdb.Path path : shortestPathViaBib.findAllPaths(bibEntry, edgeEmail)) {
                        int pathLength = path.length() + 1;
                        if (length != 0 && pathLength > length) {
                            break;
                        }
                        if (pathLength < length) {
                            // we found a shorter path via the bib entry, reset count
                            count = 0;
                        }
                        length = pathLength;
                        count++;
                    }
                }

                if (length > 0 && count > 0) {
                    String email = (String) edgeEmail.getProperty("email", "");
                    writeResultObject(jg, email, length, count);
                }
            }
        }
    }

    private void streamShortestPathsUsingHandwrittenBFS(String centerEmail, List<String> bibEntries, List<String> edgeEmails, int maxLength, JsonGenerator jg) throws IOException {
        try (Transaction tx = db.beginTx()) {
            ThreadToStatementContextBridge ctx = dbAPI.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            ReadOperations ops = ctx.get().readOperations();

            final Long centerNodeId;
            try {
                centerNodeId = nodeCache.getEmailNode(centerEmail);
            } catch (ExecutionException e) {
                return;
            }
            List<Long> bibliographyNodeIds = nodeCache.getBibliographEntryNodes(bibEntries);

            final HashLongObjMap<String> edgeEmailsByNodeId = HashLongObjMaps.newMutableMap();
            for (String edgeEmail : edgeEmails) {
                try {
                    edgeEmailsByNodeId.put(nodeCache.getEmailNode(edgeEmail), edgeEmail);
                } catch (Exception e) {
                    continue;
                }
            }

            int level = 1;
            HashLongIntMap pathsToLastLevel = HashLongIntMaps.newMutableMapOf(centerNodeId, 1);
            final LongSet previouslySeen = HashLongSets.newMutableSet();

            Cursor<NodeItem> nodeCursor;
            Cursor<RelationshipItem> relationshipCursor;
            LongIntCursor longIntCursor;

            while (level <= maxLength && !edgeEmailsByNodeId.isEmpty() && !pathsToLastLevel.isEmpty()) {
                if (level < maxLength) {
                    HashLongIntMap pathsToNextLevel = HashLongIntMaps.newMutableMap();
                    // Get nodes at next level, counting by number of times they appear
                    longIntCursor = pathsToLastLevel.cursor();
                    while (longIntCursor.moveNext()) {
                        long nodeId = longIntCursor.key();
                        int pathCount = longIntCursor.value();

                        nodeCursor = ops.nodeCursor(nodeId);
                        nodeCursor.next();
                        relationshipCursor = nodeCursor.get().relationships(Direction.BOTH);

                        while (relationshipCursor.next()) {
                            long otherId = relationshipCursor.get().otherNode(nodeId);
                            if (!previouslySeen.contains(otherId)) {
                                pathsToNextLevel.addValue(otherId, pathCount, 0);
                            }
                        }
                    }

                    if (level == 1) {
                        // Pretend there are length 1 paths to the bibliograpahy entries
                        for (Long bibId : bibliographyNodeIds) {
                            pathsToNextLevel.putIfAbsent(bibId.longValue(), 1);
                        }
                    }

                    // Now next level is current level; store visited nodes to prevent re-visiting cycles,
                    // report any target nodes that appear, then stop searching for them
                    previouslySeen.addAll(pathsToLastLevel.keySet());
                    pathsToLastLevel = pathsToNextLevel;
                    longIntCursor = pathsToLastLevel.cursor();
                    while (longIntCursor.moveNext()) {
                        long nodeId = longIntCursor.key();

                        if (edgeEmailsByNodeId.containsKey(nodeId)) {
                            String email = edgeEmailsByNodeId.remove(nodeId);
                            writeResultObject(jg, email, level, longIntCursor.value());
                        }
                    }
                } else {
                    // Last level; get nodes, but only bother to count if it's a node we care about,
                    // and look up neighbors of each edge node instead of continuing breadth-first search
                    // outwards from original center, because there are probably fewer lookups this way,
                    // and it lets us stream results
                    LongObjCursor<String> longObjCursor = edgeEmailsByNodeId.cursor();
                    while (longObjCursor.moveNext()) {
                        long nodeId = longObjCursor.key();
                        int pathCount = 0;

                        nodeCursor = ops.nodeCursor(nodeId);
                        nodeCursor.next();
                        relationshipCursor = nodeCursor.get().relationships(Direction.BOTH);

                        while (relationshipCursor.next()) {
                            long otherId = relationshipCursor.get().otherNode(nodeId);

                            if (pathsToLastLevel.containsKey(otherId)) {
                                pathCount = pathCount + pathsToLastLevel.get(otherId);
                            }
                        }

                        if (pathCount > 0) {
                            writeResultObject(jg, longObjCursor.value(), level, pathCount);
                        }
                    }
                }

                level++;
            }
        }
    }

    private static volatile IntIntMap relationships;

    private static final IntIntMap relationshipCosts(ReadOperations readOps, Map<String,Integer> costs) {
        Builder<Integer, Integer> builder = ImmutableMap.<Integer, Integer>builder();
        for (Map.Entry<String, Integer> e : costs.entrySet()) {
            builder.put(readOps.relationshipTypeGetForName(e.getKey()), e.getValue().intValue());
        }
        return relationships = HashIntIntMaps.getDefaultFactory()
            .withDefaultValue(100) // makes it too expensive to explore relationships not listed here
            .newImmutableMap(builder.build());
    }

    //Default costs
    private static final IntIntMap relationshipCosts(ReadOperations readOps) {
        if (relationships == null) {
            relationships = relationshipCosts(readOps, ImmutableMap.<String, Integer>builder()
                .put("EqualTo", 4)
                .put("HasEmail", 4)
                .put("AuthoredBy", 4)
                .put("ContainsEmail", 4)
                .put("CoAuthorOf", 4)
                .put("Follows", 4)
                .put("hasContact", 4)
                .put("HasUrl", 4)
                .build());
        }
        return relationships;
    }

    private void streamShortestPathsUsingDijkstra(String centerEmail, List<String> bibEntries, List<String> edgeEmails, int maxCost, Map<String,Integer> edgeCosts, JsonGenerator jg) {
        int centerMaxCost = Math.max(Math.min(4, maxCost), maxCost - 4);
        int edgeMaxCost = Math.max(0, maxCost - centerMaxCost);
        try (Transaction tx = db.beginTx()) {
            final Long centerNodeId;
            try {
                centerNodeId = nodeCache.getEmailNode(centerEmail);
            } catch (ExecutionException e) {
                return;
            }
            Map<Long, Integer> startNodes = HashLongIntMaps.newMutableMap();
            for (Long nodeId : nodeCache.getBibliographEntryNodes(bibEntries))  {
                startNodes.put(nodeId, 1);
            }
            startNodes.put(centerNodeId, 0);

            final HashLongObjMap<String> edgeEmailsByNodeId = HashLongObjMaps.newMutableMap();
            for (String edgeEmail : edgeEmails) {
                try {
                    edgeEmailsByNodeId.put(nodeCache.getEmailNode(edgeEmail), edgeEmail);
                } catch (Exception e) {
                    continue;
                }
            }

            ThreadToStatementContextBridge ctx = dbAPI.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            ReadOperations ops = ctx.get().readOperations();
            IntIntMap relationshipCosts = edgeCosts == null ? relationshipCosts(ops) : relationshipCosts(ops, edgeCosts);

            Dijkstra centerTraversal = new Dijkstra(ops, relationshipCosts, startNodes, centerMaxCost, new Traversal.NodeCallback() {
                public void explored(Traversal traversal, NodeItem node, long nodeId, int cost, int paths) {
                    String email = edgeEmailsByNodeId.remove(nodeId);
                    if (email != null) { //found a match!
                        try {
                            writeResultObject(jg, email, cost, paths);
                        } catch(IOException ex) {
                            traversal.finish();
                            return;
                        }
                        if (edgeEmailsByNodeId.isEmpty()) {
                            traversal.finish();
                        }
                    }
                }
            });
            centerTraversal.run();
            for (Long nodeId : edgeEmailsByNodeId.keySet()) {
                // java doesn't let you use mutable variables in a closure
                final MutableInt minCost = new MutableInt(0);
                final MutableInt totalPaths = new MutableInt(0);

                new Dijkstra(ops, relationshipCosts, ImmutableMap.<Long, Integer>of(nodeId, 0), edgeMaxCost, new Traversal.NodeCallback() {
                    public void explored(Traversal traversal, NodeItem node, long connectingNode, int cost, int paths) {
                        if (centerTraversal.hasExplored(connectingNode)) {
                            cost = centerTraversal.getCost(connectingNode) + cost;
                            paths = centerTraversal.getPaths(connectingNode) * paths;
                            if (minCost.intValue() == 0 || cost < minCost.intValue()) {
                                minCost.setValue(cost);
                                totalPaths.setValue(paths);
                            } else if (cost == minCost.intValue()) {
                                totalPaths.add(paths);
                            } else {//keep adding up connecting paths until we hit a higher cost
                                traversal.finish();
                            }
                        }
                    }
                }).run();
                if (minCost.intValue() == 0) { //we didnt find any connections
                    continue;
                }

                String email = edgeEmailsByNodeId.get(nodeId);
                try {
                    writeResultObject(jg, email, minCost.intValue(), totalPaths.intValue());
                } catch(IOException ex) {
                    return;
                }
            }
        } catch (Exception e) {
            return;
        }
    }

    private void writeResultObject(JsonGenerator jg, String email, int length, int count) throws IOException {
        jg.writeStartObject();
        jg.writeStringField("email", email);
        jg.writeNumberField("length", length);
        jg.writeNumberField("count", count);
        jg.writeEndObject();
        jg.writeRaw("\n");
        jg.flush();
    }
}
