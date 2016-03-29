package com.conveyal.taui.models;

import com.conveyal.geojson.GeometryDeserializer;
import com.conveyal.geojson.GeometrySerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.vividsolutions.jts.geom.LineString;

/**
 * Created by matthewc on 3/28/16.
 */
public class AddStops extends Modification {
    public String name;

    public String feed;
    public String[] routes;
    public String[] trips;

    public String fromStop;
    public String toStop;

    @JsonSerialize(using = GeometrySerializer.class)
    @JsonDeserialize(using = GeometryDeserializer.class)
    public LineString geometry;

    public boolean[] stops;
    public boolean[] controlPoints;
    public String[] stopIds;

    @Override
    public String getType() {
        return "add-stops";
    }
}
