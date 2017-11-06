package com.conveyal.taui.models;

/**
 * Represents a TAUI scenario
 */
public class Scenario extends Model implements Cloneable {
    /** Names of the variants of this scenario */
    public String[] variants;

    public String projectId;

    public String bundleId;

    public Scenario clone () {
        try {
            return (Scenario) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
