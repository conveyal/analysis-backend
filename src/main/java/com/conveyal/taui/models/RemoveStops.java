package com.conveyal.taui.models;

/**
 * Created by matthewc on 3/2/16.
 */
public class RemoveStops extends Modification {
    public String name;

    public String feed;

    public String[] routes;

    public String[] trips;

    public String[] stops;

    public int secondsSavedAtEachStop = 0;

    @Override
    public String getType() {
        return "remove-stops";
    }
}
