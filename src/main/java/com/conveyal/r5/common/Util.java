package com.conveyal.r5.common;

import java.util.Arrays;

public abstract class Util {

    public static String human (double n, String units) {
        String prefix = "";
        if (n > 1024) {
            n /= 1024;
            prefix = "ki";
        }
        if (n > 1024) {
            n /= 1024;
            prefix = "Mi";
        }
        if (n > 1024) {
            n /= 1024;
            prefix = "Gi";
        }
        if (n > 1024) {
            n /= 1024;
            prefix = "Ti";
        }
        return String.format("%1.1f %s%s", n, prefix, units);
    }

    /** Convenience method to create an array and fill it immediately with a single value. */
    public static int[] newIntArray (int length, int defaultValue) {
        int[] array = new int[length];
        Arrays.fill(array, defaultValue);
        return array;
    }

}
