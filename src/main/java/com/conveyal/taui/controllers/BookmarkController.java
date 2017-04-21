package com.conveyal.taui.controllers;

import com.conveyal.taui.models.Bookmark;
import com.conveyal.taui.persistence.Persistence;
import com.conveyal.taui.util.JsonUtil;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.Collection;
import java.util.UUID;

/**
 * Created by matthewc on 4/18/17.
 */
public class BookmarkController {
    public static Collection<Bookmark> getAllBookmarks (Request request, Response response) {
        return Persistence.bookmarks.getByProperty("projectId", request.params("project"));
    }

    public static Bookmark createBookmark (Request request, Response response) throws IOException {
        Bookmark bookmark = JsonUtil.objectMapper.readValue(request.body(), Bookmark.class);
        bookmark.id = UUID.randomUUID().toString();
        Persistence.bookmarks.put(bookmark.id, bookmark);
        return bookmark;
    }
}
