package com.maxdemarzi.shortest;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class Exceptions extends WebApplicationException {

    public Exceptions(int code, String error)  {
        super(new Throwable(error), Response.status(code)
                .entity("{\"error\":\"" + error + "\"}")
                .type(MediaType.APPLICATION_JSON)
                .build());

    }

    public static Exceptions invalidInput = new Exceptions(400, "Invalid Input");

    public static Exceptions missingCenterEmailParameter = new Exceptions(400, "Missing center_email Parameter.");
    public static Exceptions invalidCenterEmailParameter = new Exceptions(400, "Invalid center_email Parameter.");

    public static Exceptions missingBibliographyEntriesParameter = new Exceptions(400, "Missing bibliography_entries Parameter.");
    public static Exceptions invalidBibliographyEntriesParameter = new Exceptions(400, "Invalid bibliography_entries Parameter.");

    public static Exceptions missingEdgeEmailsParameter = new Exceptions(400, "Missing edge_email Parameter.");
    public static Exceptions invalidEdgeEmailsParameter = new Exceptions(400, "Invalid edge_email Parameter.");

    public static Exceptions missingLengthParameter = new Exceptions(400, "Missing length Parameter.");
    public static Exceptions invalidLengthParameter = new Exceptions(400, "Invalid length Parameter.");

    public static Exceptions missingCostParameter = new Exceptions(400, "Missing max_cost Parameter.");
    public static Exceptions invalidCostParameter = new Exceptions(400, "Invalid max_cost Parameter.");

    public static Exceptions timedOut = new Exceptions(420, "Timed out.");

}
