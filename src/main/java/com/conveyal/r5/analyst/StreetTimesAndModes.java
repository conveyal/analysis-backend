package com.conveyal.r5.analyst;

import com.conveyal.r5.profile.StreetMode;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;

/**
 * Contains a map from stop vertex to the clock time at which, and mode by which, the stop is reached, plus utility
 * methods for updating/processing this map.
 */
public class StreetTimesAndModes {
    public TIntObjectMap<StreetTimeAndMode> streetTimesAndModes;

    public static class StreetTimeAndMode {
        public int time;
        public StreetMode mode;

        StreetTimeAndMode(int time, StreetMode mode) {
            this.time = time;
            this.mode = mode;
        }
    };

    /**
     * Strips out mode information
     * @return map from stop vertex to time
     */
    public TIntIntMap getTimes() {
        TIntIntMap times = new TIntIntHashMap();
        streetTimesAndModes.forEachEntry((stop, timeAndMode) -> {
            times.put(stop, timeAndMode.time);
            return true; // Trove signal to continue iteration
        });
        return times;
    }

    /**
     * Merges the supplied values with the ones in this map, keeping the value with the minimum time when keys collide.
     * @param times map from stop vertex to clock time at which the stop was reached in a street search
     * @param streetMode used to obtain these times
     */
    void update(TIntIntMap times, StreetMode streetMode) {
        times.forEachEntry((stop, time) -> {
            if (!streetTimesAndModes.containsKey(stop) || time < streetTimesAndModes.get(stop).time) {
                streetTimesAndModes.put(stop, new StreetTimeAndMode(time, streetMode));
            }
            return true;
        });
    }

}



