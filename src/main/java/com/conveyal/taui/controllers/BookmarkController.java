package com.conveyal.taui.controllers;

import com.conveyal.taui.models.Bookmark;
import com.conveyal.taui.persistence.Persistence;
import com.mongodb.QueryBuilder;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.Collection;

/**
 * Created by matthewc on 4/18/17.
 */
public class BookmarkController {
    public static Collection<Bookmark> getAllBookmarks (Request request, Response response) {
        return Persistence.bookmarks.findPermitted(
                QueryBuilder.start("projectId").is(request.params("project")).get(),
                request.attribute("accessGroup")
        );
    }

    public static Bookmark createBookmark (Request request, Response response) throws IOException {
        return Persistence.bookmarks.createFromJSONRequest(request, Bookmark.class);
    }
}
