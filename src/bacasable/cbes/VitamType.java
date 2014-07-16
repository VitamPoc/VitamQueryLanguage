/**
 * This file is part of POC MongoDB ElasticSearch Project.
 *
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published
 * by the Free Software Foundation, either versionRank 3 of the License, or
 * (at your option) any later versionRank.
 *
 * POC MongoDB ElasticSearch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with POC MongoDB ElasticSearch . If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.cbes;

import java.util.Collection;
import java.util.Map;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

import fr.gouv.vitam.cbes.CouchbaseAccess.VitamCollection;
import fr.gouv.vitam.query.exception.InvalidParseOperationException;
import fr.gouv.vitam.query.exception.InvalidUuidOperationException;
import fr.gouv.vitam.query.json.JsonHandler;
import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.UUID;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * The default Vitam Type object to be stored in the database (Couchbase/ElasticSearch mode)
 *
 * @author "Frederic Bregier"
 *
 */
public abstract class VitamType {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(VitamType.class);

    /**
     * Default ID field name
     */
    public static final String ID = "_id";

    protected JsonObject json = JsonObject.empty();

    /**
     * 
     */
    public VitamType() {
    }
    /**
     * Clear all attributes
     */
    public void clear() {
        json.toMap().clear();
    }
    /**
     * This (Domain) is a root
     *
     * @throws InvalidUuidOperationException
     */
    public final void setRoot() throws InvalidUuidOperationException {
        String id = (String) json.get(ID);
        if (id == null) {
            id = new UUID().toString();
            setId(id);
        }
        GlobalDatas.ROOTS.add(id);
    }

    /**
     * Create a new ID
     */
    public final void setNewId() {
        json.put(ID, new UUID().toString());
    }

    /**
     * Set a new ID
     *
     * @param id
     */
    public final void setId(final String id) {
        json.put(ID, id);
    }

    /**
     *
     * @return the ID
     */
    public String getId() {
        return json.getString(ID);
    }

    /**
     * Update value from argument
     * @param value
     */
    public final void set(final JsonObject value) {
        json.toMap().putAll(value.toMap());
    }
    /**
     * Load from a JSON String
     *
     * @param json
     * @throws InvalidParseOperationException 
     */
    public final void load(final String json) throws InvalidParseOperationException {
        Map<String, Object> map;
        try {
            map = JsonHandler.getMapFromString(json);
            this.json.toMap().putAll(map);
            getAfterLoad();
        } catch (InvalidParseOperationException e) {
            LOGGER.error(e);
            throw new InvalidParseOperationException(e);
        }
    }

    /**
     * To be called after any automatic load or loadFromJson to
     * update HashMap values.
     */
    public void getAfterLoad() {
    }

    /**
     * To be called before any collection.insert() or update if
     * HashMap values is changed.
     */
    public void putBeforeSave() {
    }

    /**
     * Save the object. Implementation should call putBeforeSave before the real save operation
     *
     * @param dbvitam
     */
    public abstract void save(CouchbaseAccess dbvitam);

    /**
     * try to update the object if necessary (difference from the current value in the database)
     *
     * @param dbvitam
     * @return True if the object does not need any extra save operation
     */
    protected abstract boolean updated(CouchbaseAccess dbvitam);

    /**
     * load the object from the database, ignoring any previous data, except ID
     *
     * @param dbvitam
     */
    public abstract void load(CouchbaseAccess dbvitam);

    /**
     * Save the document if new, update it (keeping non set fields, replacing set fields)
     *
     * @param collection
     */
    protected final void updateOrSave(final VitamCollection collection) {
        final String id = (String) json.get(ID);
        if (id == null) {
            setNewId();
        }
        JsonDocument doc = JsonDocument.create(getId(), json);
        collection.collection.insert(doc).toBlockingObservable().first();
    }
    
    /**
     * Force the save (insert) of this document (no putBeforeSave done)
     * @param collection
     */
    protected final void forceSave(final VitamCollection collection) {
        final String id = (String) json.get(ID);
        if (id == null) {
            setNewId();
        }
        JsonDocument doc = JsonDocument.create(getId(), json);
        collection.collection.insert(doc).toBlockingObservable().first();
    }
    /**
     * Delete the current object
     *
     * @param collection
     */
    public final void delete(final VitamCollection collection) {
        collection.collection.remove(this.getId()).toBlockingObservable().first();
    }

    /**
     * Update the item (must be saved before) using the update part
     *
     * @param collection
     * @param update
     */
    protected final void update(final VitamCollection collection, final JsonObject update) {
        json.toMap().putAll(update.toMap());
        JsonDocument document = JsonDocument.create(getId(), json);
        collection.collection.upsert(document);
    }
    
    protected Object get(String name) {
        return json.get(name);
    }
    protected int getInt(String name) {
        return json.getInt(name);
    }
    protected long getLong(String name) {
        return json.getLong(name);
    }
    protected boolean getBoolean(String name) {
        return json.getBoolean(name);
    }
    protected String getString(String name) {
        return json.getString(name);
    }
    protected void put(String name, String value) {
        json.put(name, value);
    }
    protected void put(String name, int value) {
        json.put(name, value);
    }
    protected void put(String name, long value) {
        json.put(name, value);
    }
    protected void put(String name, boolean value) {
        json.put(name, value);
    }
    protected void put(String name, Collection<?> values) {
        json.put(name, JsonArray.empty().toList().addAll(values));
    }
    protected Object removeField(String name) {
        return json.toMap().remove(name);
    }
    protected boolean containsField(String name) {
        return json.toMap().containsKey(name);
    }
    protected void putAll(VitamType vt) {
        json.toMap().putAll(vt.json.toMap());
    }
    /**
     *
     * @return the bypass toString
     */
    public String toStringDirect() {
        return super.toString();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ": " + super.toString();
    }

    /**
     *
     * @return the toString for Debug mode
     */
    public String toStringDebug() {
        return this.getClass().getSimpleName() + ": " + json.get(ID);
    }

}
