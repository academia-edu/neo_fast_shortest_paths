package com.maxdemarzi.shortest;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;

public class Validators {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static HashMap getValidQueryInput(String body) throws IOException {
        HashMap input = parseInput(body);
        validateStartNodes(input);
        validateEndNodes(input);
        validateLength(input);
        return input;
    }

    public static HashMap getValidDijkstraInput(String body) throws IOException {
        HashMap input = parseInput(body);
        validateStartNodes(input);
        validateEndNodes(input);
        validateCost(input);
        return input;
    }

    private static HashMap parseInput(String body) throws IOException {
        try {
            return objectMapper.readValue(body, HashMap.class);
        } catch (Exceptions e) {
            throw Exceptions.invalidInput;
        }
    }

    private static void validateStartNodes(HashMap input) {
        // Make sure it has a center_email parameter
        if (!input.containsKey("center_email")) {
            throw Exceptions.missingCenterEmailParameter;
        }
        // Make sure it has a bibliography_entries parameter
        if (!input.containsKey("bibliography_entries")) {
            throw Exceptions.missingBibliographyEntriesParameter;
        }
        // Make sure the center_email is not blank
        if (input.get("center_email") == "") {
            throw Exceptions.invalidCenterEmailParameter;
        }
        // Make sure the bibliography_entries is not blank
        if (input.get("bibliography_entries") == "") {
            throw Exceptions.invalidBibliographyEntriesParameter;
        }
    }

    private static void validateEndNodes(HashMap input) {
        // Make sure it has a edge_emails parameter
        if (!input.containsKey("edge_emails")) {
            throw Exceptions.missingEdgeEmailsParameter;
        }
        // Make sure the edge_emails is not blank
        if (input.get("edge_emails") == "") {
            throw Exceptions.invalidEdgeEmailsParameter;
        }
    }

    private static void validateLength(HashMap input) {
        // Make sure the length is not blank
        if (!input.containsKey("length")) {
            throw Exceptions.missingLengthParameter;
        }
        // Make sure the length is not blank
        if (input.get("length") == "") {
            throw Exceptions.invalidLengthParameter;
        }
    }

    private static void validateCost(HashMap input) {
        // Make sure the max_cost is not blank
        if (!input.containsKey("max_cost")) {
            throw Exceptions.missingCostParameter;
        }
        // Make sure the max_cost is not blank
        if (input.get("max_cost") == "") {
            throw Exceptions.invalidCostParameter;
        }
    }

}
