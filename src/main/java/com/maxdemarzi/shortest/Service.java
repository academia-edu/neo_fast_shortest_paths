package com.maxdemarzi.shortest;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import net.openhft.koloboke.collect.LongCursor;
import net.openhft.koloboke.collect.map.LongObjCursor;
import net.openhft.koloboke.collect.map.LongIntCursor;
import net.openhft.koloboke.collect.map.hash.HashLongIntMap;
import net.openhft.koloboke.collect.map.hash.HashLongIntMaps;
import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import net.openhft.koloboke.collect.set.LongSet;
import net.openhft.koloboke.collect.set.hash.HashLongSets;
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

@Path("/service")
public class Service {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static GraphDatabaseService db;
    private final GraphDatabaseAPI dbAPI;

    public Service(@Context GraphDatabaseService graphDatabaseService) {
        db = graphDatabaseService;
        dbAPI = (GraphDatabaseAPI) db;
    }

    private static final LoadingCache<String, Long> emails = CacheBuilder.newBuilder()
            .maximumSize(1_000_000)
            .build(
                    new CacheLoader<String, Long>() {
                        public Long load(String email) throws Exception {
                            return getEmailNodeId(email);
                        }
                    });

    private static Long getEmailNodeId(String email) throws Exception{
        final Node node = db.findNode(Labels.Email, "email", email);
        if (node != null) {
            return node.getId();
        } else {
            throw new Exception("Email not found");
        }
    }

    private static final LoadingCache<String, Long> bibliographyEntries = CacheBuilder.newBuilder()
        .maximumSize(1_000_000)
        .build(new CacheLoader<String, Long>() {
                    public Long load(String bibId) throws Exception {
                        return getBibliographyEntryNodeId(bibId);
                    }
                });

    private static Long getBibliographyEntryNodeId(String bibId) throws Exception{
        final Node node = db.findNode(Labels.BibliographyEntry, "id", Long.valueOf(bibId));
        if (node != null) {
            return node.getId();
        } else {
            throw new Exception("BibliographyEntry not found");
        }
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
                centerNode = db.getNodeById(emails.get((String) input.get("center_email")));
            } catch (ExecutionException e) {
                return Response.ok().entity("[]").build();
            }

            for (String edgeEmail : (ArrayList<String>)input.get("edge_emails")) {
                Long edgeId;
                try {
                    edgeId = emails.get(edgeEmail);
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

    private static final List<Long> nodeIdsForBibliographyEntries(Collection<String> bibEntries) {
        List<Long> nodeIds = new ArrayList<>(bibEntries.size());
        for (String bibId : bibEntries) {
            try {
                nodeIds.add(bibliographyEntries.get(bibId));
            } catch (ExecutionException e) {
                continue;
            }
        }
        return nodeIds;
    }

    private static final List<Long> nodeIdsForEmails(Collection<String> emailSet) {
        List<Long> nodeIds = new ArrayList<>(emailSet.size());
        for (String email : emailSet) {
            try {
                nodeIds.add(emails.get(email));
            } catch (ExecutionException e) {
                continue;
            }
        }
        return nodeIds;
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
                centerNode = db.getNodeById(emails.get(centerEmail));
            } catch (ExecutionException e) {
                return;
            }
            final Collection<Node> edgeEmailNodes = nodesById(nodeIdsForEmails(edgeEmails));
            final Collection<Node> bibEntryNodes = nodesById(nodeIdsForBibliographyEntries(bibEntries));

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
                centerNodeId = emails.get(centerEmail);
            } catch (ExecutionException e) {
                return;
            }
            List<Long> bibliographyNodeIds = nodeIdsForBibliographyEntries(bibEntries);

            final HashLongObjMap<String> edgeEmailsByNodeId = HashLongObjMaps.newMutableMap();
            for (String edgeEmail : edgeEmails) {
                try {
                    edgeEmailsByNodeId.put(emails.get(edgeEmail), edgeEmail);
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
                            pathsToNextLevel.putIfAbsent(bibId, Integer.valueOf(1));
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
