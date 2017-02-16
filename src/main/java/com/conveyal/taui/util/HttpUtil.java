package com.conveyal.taui.util;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
 * Created by matthewc on 2/15/17.
 */
public class HttpUtil {
    public static final CloseableHttpClient httpClient = HttpClients.createDefault();
}
