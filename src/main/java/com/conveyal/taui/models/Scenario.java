package com.conveyal.taui.models;

import javax.persistence.Id;
import java.util.List;

/**
 * Represents a TAUI scenario
 */
public class Scenario extends Model {
    public String name;
    public List<String> modifications;
    public String graphId;
    public double lat;
    public double lon;
}
