package com.conveyal.taui.models;

import javax.persistence.Id;
import java.util.List;

/**
 * Represents a TAUI scenario
 */
public class Scenario extends Model implements Cloneable {
    public String name;

    /** Names of the variants of this scenario */
    public String[] variants;

    public String projectId;

    public String bundleId;

    /** Before we had projects, this held the group ID for this scenario. Now group ID is implied by the project ID. */
    @Deprecated
    public String group;

    public Scenario clone () {
        try {
            return (Scenario) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
