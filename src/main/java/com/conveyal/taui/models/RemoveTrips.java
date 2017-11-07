package com.conveyal.taui.models;

/**
 * Remove trips from a graph.
 */
public class RemoveTrips extends Modification {
    public String name;

    public String feed;

    public String[] routes;

    public String[] trips;

    public String[] patterns;

    public String getType () {
        return "remove-trips";
    }
}
