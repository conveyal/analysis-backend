package com.conveyal.taui.models;

import org.bson.types.ObjectId;

import javax.persistence.Id;
import java.time.Instant;

/**
 * Created by matthewc on 2/9/16.
 */
public abstract class Model {
    @Id
    public String _id;

    public String nonce;

    public String createdAt;
    public String updatedAt;

    public void updateLock() {
        this.nonce = new ObjectId().toString();
        this.updatedAt = Instant.now().toString();
    }

    public String accessGroup;
    public String createdBy;
    public String updatedBy;
}
