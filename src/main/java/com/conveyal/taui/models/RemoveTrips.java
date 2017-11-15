package com.conveyal.taui.models;

/**
 * Remove trips from a graph.
 */
public class RemoveTrips extends Modification {
    public String getType () {
        return "remove-trips";
    }

    public String feed;

    public String[] routes;

    public String[] trips;

    public String[] patterns;
}
