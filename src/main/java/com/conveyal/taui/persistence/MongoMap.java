package com.conveyal.taui.persistence;

import com.conveyal.taui.models.Bundle;
import com.conveyal.taui.models.Model;
import org.bson.types.ObjectId;
import org.mongojack.DBQuery;
import org.mongojack.JacksonDBCollection;
import org.mongojack.WriteResult;

import java.util.*;

/**
 * Make an attempt at simulating a MapDB-style interface
 */
public class MongoMap<V extends Model> implements Map<String, V> {
    private JacksonDBCollection<V, String> wrappedCollection;

    public MongoMap (JacksonDBCollection<V, String> wrappedCollection) {
        this.wrappedCollection = wrappedCollection;
    }

    public int size() {
        return (int) wrappedCollection.getCount();
    }

    public boolean isEmpty() {
        return wrappedCollection.getCount() > 0;
    }

    public boolean containsKey(Object key) {
        if (key instanceof String)
            return wrappedCollection.findOneById((String) key) != null;
        else return false;
    }

    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    public V get(Object key) {
        if (key instanceof String)
            return wrappedCollection.findOneById((String) key);
        else return null;
    }

    /** Get all objects where property == value */
    public Collection<V> getByProperty (String property, Object value) {
        return wrappedCollection.find().is(property, value).toArray();
    }

    public V put(String key, V value) {
        if (!key.equals(value.id)) throw new IllegalArgumentException("Model ID and key must match!");

        // TODO returning old value is needed to truly implement map interface, but may be slowing down puts
        V ret = get(key);

        if (ret != null)
            wrappedCollection.updateById(value.id, value);
        else
            wrappedCollection.insert(value);

        return ret;
    }

    public V remove(Object key) {
        if (key instanceof String) {
            V v = wrappedCollection.findOneById((String) key);
            wrappedCollection.removeById((String) key);
            return v;
        }
        else return null;
    }

    public void putAll(Map<? extends String, ? extends V> m) {
        m.forEach(this::put);
    }

    public void clear() {
        Iterator<V> it = wrappedCollection.find().iterator();

        while (it.hasNext()) {
            // TODO will this work?
            it.remove();
        }
    }

    public Set<String> keySet() {
        Iterator<V> it = wrappedCollection.find().iterator();

        Set<String> ret = new HashSet<>();

        while (it.hasNext()) {
            ret.add(it.next().id);
        }

        // TODO cheating. Technically changes here are supposed to be reflected in the map
        return Collections.unmodifiableSet(ret);
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

    public Set<Entry<String, V>> entrySet() {
        Iterator<V> it = wrappedCollection.find().iterator();

        Set<Entry<String, V>> ret = new HashSet<>();

        while (it.hasNext()) {
            V val = it.next();
            ret.add(new AbstractMap.SimpleImmutableEntry<>(val.id, val));
        }

        // TODO cheating. Technically changes here are supposed to be reflected in the map
        return Collections.unmodifiableSet(ret);
    }
}
