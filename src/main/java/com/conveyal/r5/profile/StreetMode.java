package com.conveyal.r5.profile;

/**
 * Represents a travel mode used to traverse edges in the street graph.
 * Permissions on edges will allow or disallow traversal by these modes, and edges may be traversed at different
 * speeds depending on the selected mode.
 */
public enum StreetMode {
    WALK(0),
    BICYCLE(1),
    CAR(2);

    final int value;

    StreetMode(int value) {
        this.value = value;
    }
}
