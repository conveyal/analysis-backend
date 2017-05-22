package com.conveyal.taui.util;

import java.io.Serializable;
import java.net.URL;

/**
 * Created by matthewc on 5/22/17.
 */
public class WrappedURL implements Serializable {
    public String url;

    public WrappedURL(String url) {
        this.url = url;
    }
    public WrappedURL (URL url) {
        this.url = url.toString();
    }
}
