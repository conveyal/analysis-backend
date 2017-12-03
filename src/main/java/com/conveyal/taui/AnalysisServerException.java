package com.conveyal.taui;

import graphql.GraphQLError;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Response;

import java.io.IOException;
import java.util.List;

public class AnalysisServerException extends RuntimeException {
    private static final Logger LOG = LoggerFactory.getLogger(AnalysisServerException.class);

    public int httpCode;
    public TYPE type;
    public String message;

    public enum TYPE {
        BAD_REQUEST,
        BROKER,
        FILE_UPLOAD,
        FORBIDDEN,
        GRAPHQL,
        JSON_PARSING,
        NONCE,
        NOT_FOUND,
        UNAUTHORIZED,
        UNKNOWN;
    }

    public static AnalysisServerException BadRequest(String message) {
        return new AnalysisServerException(TYPE.BAD_REQUEST, message, 400);
    }

    public static AnalysisServerException Broker(String message) {
        return new AnalysisServerException(TYPE.BROKER, message, 400);
    }

    public static AnalysisServerException FileUpload(String message) {
        return new AnalysisServerException(TYPE.FILE_UPLOAD, message, 400);
    }

    public static AnalysisServerException Forbidden(String message) {
        return new AnalysisServerException(TYPE.FORBIDDEN, message, 403);
    }

    public static AnalysisServerException GraphQL(List<GraphQLError> errors) {
        return new AnalysisServerException(
                TYPE.GRAPHQL,
                errors
                    .stream()
                    .map(e -> e.getMessage())
                    .reduce("", (a, b) -> a + " " + b),
                400
        );
    }

    public static AnalysisServerException JSONParsing(IOException e) {
        return new AnalysisServerException(TYPE.JSON_PARSING, "Error parsing JSON received from the client. " + e.getMessage(), 400);
    }

    public static AnalysisServerException Nonce() {
        return new AnalysisServerException(TYPE.NONCE, "The data you attempted to change is out of date and could not be updated.", 400);
    }

    public static AnalysisServerException NotFound(String message) {
        return new AnalysisServerException(TYPE.NOT_FOUND, message, 404);
    }

    public static AnalysisServerException Unauthorized(String message) {
        return new AnalysisServerException(TYPE.UNAUTHORIZED, message, 401);
    }

    public static AnalysisServerException Unknown(Exception e) {
        return new AnalysisServerException(TYPE.UNKNOWN, e.getMessage(), 400);
    }

    public static AnalysisServerException Unknown(String message) {
        return new AnalysisServerException(TYPE.UNKNOWN, message, 400);
    }

    public AnalysisServerException(Exception e, String message) {
        this(message);
        LOG.error(e.getMessage());
    }

    public AnalysisServerException(String message) {
        this(TYPE.UNKNOWN, message, 400);
    }

    public AnalysisServerException(AnalysisServerException.TYPE t, String m, int c) {
        httpCode = c;
        type = t;
        message = m;
    }

    public void respond (Response response) {
        String stack = ExceptionUtils.getStackTrace(this);

        LOG.error("AnalysisServerException thrown, type: {}, message: {}", type, message);
        LOG.error(stack);

        JSONObject body = new JSONObject();
        body.put("type", type.name());
        body.put("message", message);
        body.put("stackTrace", stack);

        response.status(httpCode);
        response.type("application/json");
        response.body(body.toJSONString());
    }
}
