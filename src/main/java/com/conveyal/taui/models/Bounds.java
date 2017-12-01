package com.conveyal.taui.models;

public class Bounds {
    public double north, east, south, west;

    @Override
    public boolean equals (Object other) {
        return equals(other, 0D);
    }

    public boolean equals (Object other, double tolerance) {
        if (!Bounds.class.isInstance(other)) return false;
        Bounds o = (Bounds) other;
        return Math.abs(north - o.north) <= tolerance && Math.abs(east - o.east) <= tolerance &&
                Math.abs(south - o.south) <= tolerance && Math.abs(west - o.west) <= tolerance;
    }
}
