package com.conveyal.taui.models;

public class Progress extends Model {
    public int tasksComplete;
    public int tasksTotal;

    public Status status;
    public Types type;

    public enum Status {
        STARTED,
        PROGRESSING,
        COMPLETED,
        FAILED
    }

    public enum Types {
        RESOURCE
    }
}
