package com.conveyal.taui.persistence;

import com.conveyal.r5.common.JsonUtilities;
import com.conveyal.taui.AnalysisServerException;
import com.conveyal.taui.models.Model;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import org.bson.types.ObjectId;
import org.mongojack.DBCursor;
import org.mongojack.DBSort;
import org.mongojack.JacksonDBCollection;
import org.mongojack.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * An attempt at simulating a MapDB-style interface, for storing Java objects in MongoDB.
 * Note this used to implement Map, but that predates generics, so it is more typesafe not to.
 * TODO this is using org.mongojack.JacksonDBCollection. I believe Mongo Java client library now provides POJO storage.
 */
public class MongoMap<V extends Model> {
    private static Logger LOG = LoggerFactory.getLogger(MongoMap.class);

    private JacksonDBCollection<V, String> wrappedCollection;
    private Class<V> type;

    public MongoMap (JacksonDBCollection<V, String> wrappedCollection, Class<V> type) {
        this.type = type;
        this.wrappedCollection = wrappedCollection;
    }

    public int size() {
        return (int) wrappedCollection.getCount();
    }

    public V findByIdFromRequestIfPermitted(Request request) {
        return findByIdIfPermitted(request.params("_id"), request.attribute("accessGroup"));
    }

    public V findByIdIfPermitted(String id, String accessGroup) {
        V result = wrappedCollection.findOneById(id);

        if (result == null) {
            throw AnalysisServerException.notFound("The data you requested could not be found.");
        } else if (!accessGroup.equals(result.accessGroup)) {
            throw AnalysisServerException.forbidden("You do not have permission to access this data.");
        } else {
            return result;
        }
    }

    public V get(String key) {
        return wrappedCollection.findOneById(key);
    }

    public Collection<V> findAllForRequest(Request req) {
        return find(QueryBuilder.start("accessGroup").is(req.attribute("accessGroup")).get()).toArray();
    }

    public Collection<V> findPermitted(DBObject query, String accessGroup) {
        return find(QueryBuilder.start().and(
                query,
                QueryBuilder.start("accessGroup").is(accessGroup).get()
        ).get()).toArray();
    }

    public Collection<V> findPermittedForQuery (Request req) {
        QueryBuilder query = QueryBuilder.start();
        req.queryParams().forEach(name -> {
            query.and(name).is(req.queryParams(name));
        });

        return findPermitted(query.get(), req.attribute("accessGroup"));
    }

    /**
     * All Models have a createdAt field. By default, sort by that field.
     */
    public DBCursor<V> find(DBObject query) {
        return wrappedCollection.find(query).sort(DBSort.desc("createdAt"));
    }

    /** Get all objects where property == value */
    public Collection<V> getByProperty (String property, Object value) {
        return wrappedCollection.find().is(property, value).toArray();
    }

    public V createFromJSONRequest(Request request) throws IOException {
        V json = JsonUtilities.objectMapper.readValue(request.body(), this.type);

        // Set `createdBy` and `accessGroup`
        json.accessGroup = request.attribute("accessGroup");
        json.createdBy = request.attribute("email");

        return create(json);
    }

    public V create(V value) {
        // Create an ID
        value._id = new ObjectId().toString();

        // Set updated
        value.updateLock();

        // Set `createdAt` to `updatedAt` since it was first creation
        value.createdAt = value.updatedAt;

        // Set `updatedBy` to whomever created it
        value.updatedBy = value.createdBy;

        // Insert into the DB
        wrappedCollection.insert(value);

        return value;
    }

    public V updateFromJSONRequest(Request request) throws IOException {
        V json = JsonUtilities.objectMapper.readValue(request.body(), this.type);
        // Add the additional check for the same access group
        return updateByUserIfPermitted(json, request.attribute("email"), request.attribute("accessGroup"));
    }

    public V updateByUserIfPermitted(V value, String updatedBy, String accessGroup) {
        // Set `updatedBy`
        value.updatedBy = updatedBy;

        return put(value, QueryBuilder.start("accessGroup").is(accessGroup).get());
    }

    public V put(String key, V value) {
        if (key != value._id) throw AnalysisServerException.badRequest("ID does not match");
        return put(value, null);
    }

    public V put(V value) {
        return put(value, null);
    }

    public V put(V value, DBObject optionalQuery) {
        String currentNonce = value.nonce;

        // Only update if the nonce is the same
        QueryBuilder query = QueryBuilder.start().and(
                QueryBuilder.start("_id").is(value._id).get(),
                QueryBuilder.start("nonce").is(currentNonce).get()
        );

        if (optionalQuery != null) query.and(optionalQuery);

        // Update the locking variables
        value.updateLock();

        // Set `createdAt` and `createdBy` if they have never been set
        if (value.createdAt == null) value.createdAt = value.updatedAt;
        if (value.createdBy == null) value.createdBy = value.updatedBy;

        // Convert the model into a db object
        BasicDBObject dbObject = JsonUtilities.objectMapper.convertValue(value, BasicDBObject.class);

        // Update
        V result = wrappedCollection.findAndModify(query.get(), null, null, false, dbObject, true, false);

        // If it doesn't result in an update, probably throw an error
        if (result == null) {
            result = wrappedCollection.findOneById(value._id);
            if (result == null) {
                throw AnalysisServerException.notFound("The data you attempted to update could not be found. ");
            } else if (!currentNonce.equals(result.nonce)) {
                throw AnalysisServerException.nonce();
            } else {
                throw AnalysisServerException.forbidden("The data you attempted to update is not in your access group.");
            }
        }

        // Log the result
        LOG.info("{} {} updated by {} ({})", result.toString(), result.name, result.updatedBy, result.accessGroup);

        // Return the object that was updated
        return result;
    }

    /**
     * Insert without updating the nonce or updateBy/updatedAt
     * @return
     */
    public V modifiyWithoutUpdatingLock (V value) {
        wrappedCollection.updateById(value._id, value);

        return value;
    }

    public V removeIfPermitted(String key, String accessGroup) {
        V result = wrappedCollection.findAndRemove(QueryBuilder.start().and(
                QueryBuilder.start("_id").is(key).get(),
                QueryBuilder.start("accessGroup").is(accessGroup).get()
        ).get());

        if (result == null) {
            throw AnalysisServerException.notFound("The data you attempted to remove could not be found.");
        }

        return result;
    }

    public V remove(String key) {
        WriteResult<V, String> result = wrappedCollection.removeById(key);
        LOG.info(result.toString());
        if (result.getN() == 0) {
            throw AnalysisServerException.notFound(String.format("The data for _id %s does not exist", key));
        }

        return null;
    }

    public Collection<V> values() {
        Iterator<V> it = wrappedCollection.find().iterator();

        Set<V> ret = new HashSet<>();

        while (it.hasNext()) {
            ret.add(it.next());
        }

        // TODO cheating. Technically changes here are supposed to be reflected in the map
        return Collections.unmodifiableSet(ret);
    }

}
