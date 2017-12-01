package com.conveyal.taui.models;

import org.bson.types.ObjectId;

import javax.persistence.Id;
import java.util.Date;

/**
 * Created by matthewc on 2/9/16.
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
