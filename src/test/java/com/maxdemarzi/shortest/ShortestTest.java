package com.maxdemarzi.shortest;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ShortestTest {

    final static ObjectMapper mapper = new ObjectMapper();

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withExtension("/v1", Service.class);

    @Test
    public void shouldFindShortestPathOne() {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query").toString(),
                QUERY_ONE_MAP);

        ArrayList actual = response.content();
        ArrayList<HashMap> expected = new ArrayList<HashMap>() {{ add(ONE_MAP); }};
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void shouldFindShortestPathTwo() {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query").toString(),
                QUERY_TWO_MAP);

        ArrayList actual = response.content();
        ArrayList<HashMap> expected = new ArrayList<HashMap>() {{
            add(ONE_MAP);
            add(TWO_MAP);
        }};
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void shouldFindShortestPathThree() {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query").toString(),
                QUERY_THREE_MAP);

        ArrayList actual = response.content();
        ArrayList<HashMap> expected = new ArrayList<HashMap>() {{
            add(THREE_MAP);
        }};
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void shouldDealWithMissingEdgeEmail() {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query").toString(),
                QUERY_FOUR_MAP);

        ArrayList actual = response.content();
        ArrayList<HashMap> expected = new ArrayList<HashMap>() {{
            add(FOUR_MAP);
        }};
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void shouldDealWithMissingCenterEmail() {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query").toString(),
                QUERY_NO_CENTER_EMAIL_MAP);

        ArrayList actual = response.content();
        ArrayList<HashMap> expected = new ArrayList<HashMap>();
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void shouldIgnoreWithNoPath() {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query").toString(),
                QUERY_NO_PATH_MAP);

        ArrayList actual = response.content();
        ArrayList<HashMap> expected = new ArrayList<HashMap>();
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void shouldStreamFindShortestPathOne() throws Exception {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query_streaming").toString(),
                QUERY_ONE_MAP);

        String raw = response.rawContent();
        Map<String,Object> actual = mapper.readValue(raw, Map.class);
        assertEquals(ONE_MAP, actual);
    }

    @Test
    public void shouldStreamFindShortestPathTwo() throws Exception {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query_streaming").toString(),
                QUERY_TWO_MAP);

        ArrayList actual = parseNewlineSeparated(response);

        ArrayList<HashMap> expected = new ArrayList<HashMap>() {{
            add(ONE_MAP);
            add(TWO_MAP);
        }};
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void shouldFindShortestPathByCountersOne() throws Exception {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query_counters").toString(),
                QUERY_ONE_MAP);

        ArrayList actual = parseNewlineSeparated(response);
        ArrayList<HashMap> expected = new ArrayList<HashMap>() {{ add(ONE_MAP); }};
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void shouldFindShortestPathByCountersTwo() throws Exception {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query_counters").toString(),
                QUERY_TWO_MAP);

        ArrayList actual = parseNewlineSeparated(response);
        ArrayList<HashMap> expected = new ArrayList<HashMap>() {{
            add(ONE_MAP);
            add(TWO_MAP);
        }};
        assertArrayEquals(expected.toArray(), actual.toArray());

    }

    @Test
    public void shouldFindShortestPathByCountersThree() throws Exception {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query_counters").toString(),
                QUERY_THREE_MAP);

        ArrayList actual = parseNewlineSeparated(response);
        ArrayList<HashMap> expected = new ArrayList<HashMap>() {{
            add(THREE_MAP);
        }};
        assertArrayEquals(expected.toArray(), actual.toArray());

    }

    @Test
    public void shouldDealWithMissingEmailsByCounters() throws Exception {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query_counters").toString(),
                QUERY_FOUR_MAP);

        ArrayList actual = parseNewlineSeparated(response);
        ArrayList<HashMap> expected = new ArrayList<HashMap>() {{
            add(FOUR_MAP);
        }};
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void shouldFindShortestPathByCountersFive() throws Exception {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query_counters").toString(),
                QUERY_FIVE_MAP);

        ArrayList actual = parseNewlineSeparated(response);
        ArrayList<HashMap> expected = new ArrayList<HashMap>() {{
            add(FIVE_MAP);
        }};
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void shouldFindShortestPathByEither() throws Exception {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query_either").toString(),
                QUERY_ONE_MAP);

        ArrayList actual = parseNewlineSeparated(response);
        ArrayList<HashMap> expected = new ArrayList<HashMap>() {{ add(ONE_MAP); }};
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void shouldFindShortestPathViaBibOne() throws Exception {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query_streaming").toString(),
                QUERY_BIB_ONE_MAP);

        String raw = response.rawContent();
        Map<String,Object> actual = mapper.readValue(raw, Map.class);
        assertEquals(BIB_ONE_MAP, actual);
    }

    @Test
    public void shouldFindShortestPathViaBibTwo() throws Exception {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query_counters").toString(),
                QUERY_BIB_TWO_MAP);

        String raw = response.rawContent();
        Map<String,Object> actual = mapper.readValue(raw, Map.class);
        assertEquals(BIB_TWO_MAP, actual);
    }

    @Test
    public void shouldFindShortestPathViaBibThree() throws Exception {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query_streaming").toString(),
                QUERY_BIB_THREE_MAP);

        String raw = response.rawContent();
        Map<String,Object> actual = mapper.readValue(raw, Map.class);
        assertEquals(BIB_THREE_MAP, actual);
    }

    // Dijkstra Tests

    @Test
    public void dijkstraShouldFindShortestPathOne() throws Exception {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query_shortest").toString(),
                DIJKSTRA_QUERY_ONE_MAP);

        ArrayList actual = parseNewlineSeparated(response);
        ArrayList<HashMap> expected = new ArrayList<HashMap>() {{ add(DIJKSTRA_ONE_MAP); }};
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void dijkstraShouldFindShortestPathTwo() throws Exception {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query_shortest").toString(),
                DIJKSTRA_QUERY_TWO_MAP);

        ArrayList actual = parseNewlineSeparated(response);
        ArrayList<HashMap> expected = new ArrayList<HashMap>() {{
            add(DIJKSTRA_ONE_MAP);
            add(DIJKSTRA_TWO_MAP);
        }};
        assertArrayEquals(expected.toArray(), actual.toArray());

    }

    @Test
    public void dijkstraShouldFindShortestPathThree() throws Exception {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query_shortest").toString(),
                DIJKSTRA_QUERY_THREE_MAP);

        ArrayList actual = parseNewlineSeparated(response);
        ArrayList<HashMap> expected = new ArrayList<HashMap>() {{
            add(DIJKSTRA_THREE_MAP);
        }};
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void dijkstraShouldDealWithMissingEmails() throws Exception {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query_shortest").toString(),
                DIJKSTRA_QUERY_FOUR_MAP);

        ArrayList actual = parseNewlineSeparated(response);
        ArrayList<HashMap> expected = new ArrayList<HashMap>() {{
            add(DIJKSTRA_FOUR_MAP);
        }};
        assertArrayEquals(expected.toArray(), actual.toArray());
    }

    @Test
    public void dijkstraShouldFindShortestPathViaBibTwo() throws Exception {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/v1/service/query_shortest").toString(),
                DIJKSTRA_QUERY_BIB_MAP);

        String raw = response.rawContent();
        Map<String,Object> actual = mapper.readValue(raw, Map.class);
        assertEquals(DIJKSTRA_BIB_MAP, actual);
    }

    private ArrayList parseNewlineSeparated(HTTP.Response response) throws Exception {
        String raw = response.rawContent();
        String[] lines = raw.split("\n");

        ArrayList actual = new ArrayList();
        for (String line : lines) {
            if (!line.trim().isEmpty()) {
                actual.add(mapper.readValue(line, Map.class));
            }
        }
        return actual;
    }

    public static final String MODEL_STATEMENT =
            new StringBuilder()
                    .append("CREATE (start:Email {email:'start@maxdemarzi.com'})")
                    .append("CREATE (one:Email {email:'one@maxdemarzi.com'})")
                    .append("CREATE (two:Email {email:'two@maxdemarzi.com'})")
                    .append("CREATE (three:Email {email:'three@maxdemarzi.com'})")
                    .append("CREATE (four:Email {email:'four@maxdemarzi.com'})")
                    .append("CREATE (five:Email {email:'five@maxdemarzi.com'})")
                    .append("CREATE (nope:Email {email:'unconnected@maxdemarzi.com'})")
                    .append("CREATE (six:Email {email:'six@maxdemarzi.com'})")
                    .append("CREATE (seven:Email {email:'seven@maxdemarzi.com'})")
                    .append("CREATE (eight:Email {email:'eight@maxdemarzi.com'})")
                    .append("CREATE (oneBibMail:Email {email:'onebibmail@maxdemarzi.com'})")
                    .append("CREATE (twoBibMail:Email {email:'twobibmail@maxdemarzi.com'})")
                    .append("CREATE (threeBibMail:Email {email:'threebibmail@maxdemarzi.com'})")
                    .append("CREATE (start)-[:Follows]->(one)")
                    .append("CREATE (one)-[:hasContact]->(two)")
                    .append("CREATE (one)-[:HasEmail]->(three)")
                    .append("CREATE (one)-[:HasUrl]->(four)")
                    .append("CREATE (three)<-[:ContainsEmail]-(five)")
                    .append("CREATE (four)<-[:CoAuthorOf]-(five)")
                    .append("CREATE (two)-[:EqualTo]->(six)")
                    .append("CREATE (six)-[:hasContact]->(seven)")
                    .append("CREATE (seven)-[:hasContact]->(eight)")
                    .append("CREATE (oneBib:BibliographyEntry {id: 1})")
                    .append("CREATE (oneBib)-[:hasContact]->(oneBibMail)")
                    .append("CREATE (twoBibMail)-[:hasContact]->(oneBibMail)")
                    .append("CREATE (threeBibMail)-[:AuthoredBy]->(twoBibMail)")
                    .toString();

    public static HashMap<String, Object> QUERY_ONE_MAP = new HashMap<String, Object>(){{
        put("center_email", "start@maxdemarzi.com");
        put("bibliography_entries", new ArrayList<String>());
        put("edge_emails", new ArrayList<String>() {{  add("one@maxdemarzi.com");} });
        put("length", 4);
    }};

    static HashMap<String, Object> ONE_MAP = new HashMap<String, Object>(){{
        put("email", "one@maxdemarzi.com");
        put("length", 1);
        put("count", 1);
    }};

    public static HashMap<String, Object> QUERY_TWO_MAP = new HashMap<String, Object>(){{
        put("center_email", "start@maxdemarzi.com");
        put("bibliography_entries", new ArrayList<String>());
        put("edge_emails", new ArrayList<String>() {{
            add("one@maxdemarzi.com");
            add("two@maxdemarzi.com");
        }});
        put("length", 4);
    }};

    static HashMap<String, Object> TWO_MAP = new HashMap<String, Object>(){{
        put("email", "two@maxdemarzi.com");
        put("length", 2);
        put("count", 1);
    }};

    public static HashMap<String, Object> QUERY_THREE_MAP = new HashMap<String, Object>(){{
        put("center_email", "start@maxdemarzi.com");
        put("bibliography_entries", new ArrayList<String>());
        put("edge_emails", new ArrayList<String>() {{
            add("five@maxdemarzi.com");
        }});
        put("length", 4);
    }};

    static HashMap<String, Object> THREE_MAP = new HashMap<String, Object>(){{
        put("email", "five@maxdemarzi.com");
        put("length", 3);
        put("count", 2);
    }};

    public static HashMap<String, Object> QUERY_FOUR_MAP = new HashMap<String, Object>(){{
        put("center_email", "start@maxdemarzi.com");
        put("bibliography_entries", new ArrayList<String>());
        put("edge_emails", new ArrayList<String>() {{
            add("five@maxdemarzi.com");
            add("sixty@maxdemarzi.com");
        }});
        put("length", 4);
    }};

    public static HashMap<String, Object> QUERY_NO_CENTER_EMAIL_MAP = new HashMap<String, Object>(){{
        put("center_email", "missing@maxdemarzi.com");
        put("bibliography_entries", new ArrayList<String>());
        put("edge_emails", new ArrayList<String>() {{
            add("five@maxdemarzi.com");
            add("start@maxdemarzi.com");
        }});
        put("length", 4);
    }};

    static HashMap<String, Object> FOUR_MAP = new HashMap<String, Object>(){{
        put("email", "five@maxdemarzi.com");
        put("length", 3);
        put("count", 2);
    }};

    public static HashMap<String, Object> QUERY_NO_PATH_MAP = new HashMap<String, Object>(){{
        put("center_email", "start@maxdemarzi.com");
        put("bibliography_entries", new ArrayList<String>());
        put("edge_emails", new ArrayList<String>() {{  add("unconnected@maxdemarzi.com");} });
        put("length", 4);
    }};

    public static HashMap<String, Object> QUERY_FIVE_MAP = new HashMap<String, Object>(){{
        put("center_email", "start@maxdemarzi.com");
        put("bibliography_entries", new ArrayList<String>());
        put("edge_emails", new ArrayList<String>() {{
            add("seven@maxdemarzi.com");
            add("eight@maxdemarzi.com");
        }});
        put("length", 4);
    }};

    static HashMap<String, Object> FIVE_MAP = new HashMap<String, Object>(){{
        put("email", "seven@maxdemarzi.com");
        put("length", 4);
        put("count", 1);
    }};

    public static HashMap<String, Object> QUERY_BIB_ONE_MAP = new HashMap<String, Object>(){{
        put("center_email", "start@maxdemarzi.com");
        put("bibliography_entries", new ArrayList<String>() {{ add("1");} });
        put("edge_emails", new ArrayList<String>() {{
            add("onebibmail@maxdemarzi.com");
        }});
        put("length", 4);
    }};

    static HashMap<String, Object> BIB_ONE_MAP = new HashMap<String, Object>(){{
        put("email", "onebibmail@maxdemarzi.com");
        put("length", 2);
        put("count", 1);
    }};

    public static HashMap<String, Object> QUERY_BIB_TWO_MAP = new HashMap<String, Object>(){{
        put("center_email", "start@maxdemarzi.com");
        put("bibliography_entries", new ArrayList<String>() {{ add("1");} });
        put("edge_emails", new ArrayList<String>() {{
            add("twobibmail@maxdemarzi.com");
        }});
        put("length", 4);
    }};

    static HashMap<String, Object> BIB_TWO_MAP = new HashMap<String, Object>(){{
        put("email", "twobibmail@maxdemarzi.com");
        put("length", 3);
        put("count", 1);
    }};

    public static HashMap<String, Object> QUERY_BIB_THREE_MAP = new HashMap<String, Object>(){{
        put("center_email", "start@maxdemarzi.com");
        put("bibliography_entries", new ArrayList<String>() {{ add("1");} });
        put("edge_emails", new ArrayList<String>() {{
            add("threebibmail@maxdemarzi.com");
        }});
        put("length", 4);
    }};

    static HashMap<String, Object> BIB_THREE_MAP = new HashMap<String, Object>(){{
        put("email", "threebibmail@maxdemarzi.com");
        put("length", 4);
        put("count", 1);
    }};

    // Dijkstra stuff

    static HashMap<String, Object> DIJKSTRA_QUERY_ONE_MAP = new HashMap<String, Object>(){{
        put("center_email", "start@maxdemarzi.com");
        put("bibliography_entries", new ArrayList<String>());
        put("edge_emails", new ArrayList<String>() {{  add("one@maxdemarzi.com");} });
        put("length", 16);
    }};

    static HashMap<String, Object> DIJKSTRA_ONE_MAP = new HashMap<String, Object>(){{
        put("email", "one@maxdemarzi.com");
        put("length", 4);
        put("count", 1);
    }};

    static HashMap<String, Object> DIJKSTRA_QUERY_TWO_MAP = new HashMap<String, Object>(){{
        put("center_email", "start@maxdemarzi.com");
        put("bibliography_entries", new ArrayList<String>());
        put("edge_emails", new ArrayList<String>() {{
            add("one@maxdemarzi.com");
            add("two@maxdemarzi.com");
        }});
        put("length", 16);
    }};

    static HashMap<String, Object> DIJKSTRA_TWO_MAP = new HashMap<String, Object>(){{
        put("email", "two@maxdemarzi.com");
        put("length", 8);
        put("count", 1);
    }};

    static HashMap<String, Object> DIJKSTRA_QUERY_THREE_MAP = new HashMap<String, Object>(){{
        put("center_email", "start@maxdemarzi.com");
        put("bibliography_entries", new ArrayList<String>());
        put("edge_emails", new ArrayList<String>() {{
            add("five@maxdemarzi.com");
        }});
        put("length", 16);
    }};

    static HashMap<String, Object> DIJKSTRA_THREE_MAP = new HashMap<String, Object>(){{
        put("email", "five@maxdemarzi.com");
        put("length", 8);
        put("count", 1);
    }};

    static HashMap<String, Object> DIJKSTRA_QUERY_FOUR_MAP = new HashMap<String, Object>(){{
        put("center_email", "start@maxdemarzi.com");
        put("bibliography_entries", new ArrayList<String>());
        put("edge_emails", new ArrayList<String>() {{
            add("five@maxdemarzi.com");
            add("sixty@maxdemarzi.com");
        }});
        put("length", 16);
    }};

    static HashMap<String, Object> DIJKSTRA_FOUR_MAP = new HashMap<String, Object>(){{
        put("email", "five@maxdemarzi.com");
        put("length", 8);
        put("count", 1);
    }};

    static HashMap<String, Object> DIJKSTRA_QUERY_BIB_MAP = new HashMap<String, Object>(){{
        put("center_email", "start@maxdemarzi.com");
        put("bibliography_entries", new ArrayList<String>() {{ add("1");} });
        put("edge_emails", new ArrayList<String>() {{
            add("twobibmail@maxdemarzi.com");
        }});
        put("length", 16);
    }};

    static HashMap<String, Object> DIJKSTRA_BIB_MAP = new HashMap<String, Object>(){{
        put("email", "twobibmail@maxdemarzi.com");
        put("length", 9);
        put("count", 1);
    }};
}
