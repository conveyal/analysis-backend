package com.conveyal.taui.models;

import javax.persistence.Id;

/**
 * Created by matthewc on 2/9/16.
 */
public abstract class Model {
    @Id
    public String id;
}
