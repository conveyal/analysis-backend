package com.conveyal.taui.models;

import org.bson.types.ObjectId;

import javax.persistence.Id;
import java.util.Date;

/**
 * Shared superclass for data model classes that are serialized to communicate between the UI and the backend,
 * and generally stored in the application MongoDB.
 * Other objects that are serialized and sent to the workers are not subclasses of this.
 */
public abstract class Model implements Cloneable {
    @Id
    public String _id;

    public String name;

    public String nonce;

    public Date createdAt;
    public Date updatedAt;

    public void updateLock() {
        this.nonce = new ObjectId().toString();
        this.updatedAt = new Date();
    }

    public String accessGroup;
    public String createdBy;
    public String updatedBy;
}
