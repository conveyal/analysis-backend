package com.conveyal.taui.models;

import javax.persistence.Id;
import java.util.List;

/**
 * Represents a TAUI scenario
 */
public class Scenario extends Model {
    public String name;

    /** The group this scenario is associated with */
    public String group;

    public String bundleId;
    public double lat;
    public double lon;
}
