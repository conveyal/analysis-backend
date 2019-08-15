package com.conveyal.taui.models;

import java.util.ArrayList;
import java.util.List;

public class Task extends Model {
    // To enable querying {regionId, type} to get progress for a region. If unset, still relevant to the accessGroup.
    public String regionId;

    /**
     * Store related model identifier for linking (resource._id, regionalAnalysis._id, etc.) and therefore will not need
     * to store a `taskId` on other models.
     */
    public String modelId;

    // If task has a known quantity of steps, use these fields to show user's a percentage or progress bar.
    public int complete = 0;
    public int total = 0;

    // Latest status text correlates to the current Stage. Store old messages as they may be relevant and specific to this task.
    public List<String> statuses = new ArrayList();

    public Stage stage = Stage.UNBEGUN;
    public enum Stage {
        UNBEGUN,
        PROGRESSING,
        FINISHED,
        FAILED
    }

    public Types type = Types.UNKNOWN;
    public enum Types {
        REGIONAL_ANALYSIS,
        RESOURCE,
        UNKNOWN
    }
}
