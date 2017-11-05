package com.conveyal.taui.util;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
 * Created by matthewc on 2/15/17.
 */
public class HttpUtil {
    private static final RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(60 * 1000)
            .setSocketTimeout(60 * 1000)
            .build(); // time out after one minute, prevent connections from becoming blocked.

    public static final CloseableHttpClient httpClient = HttpClients.custom()
            .setMaxConnPerRoute(1024)
            .setMaxConnTotal(2048)
            .setDefaultRequestConfig(requestConfig)
            .build();

}
