package com.conveyal.taui.models;

import javax.persistence.Id;
import java.util.List;

/**
 * Represents a TAUI scenario
 */
public class Scenario extends Model {
    public String name;
    public List<String> modifications;

    /** the names of the variants of this scenario */
    public String[] variants;

    public String bundleId;
    public double lat;
    public double lon;
}
