/**
   This file is part of POC MongoDB ElasticSearch Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either versionRank 3 of the License, or
   (at your option) any later versionRank.

   POC MongoDB ElasticSearch is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with POC MongoDB ElasticSearch .  If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.cdbes;


import org.bson.BSONObject;

import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import fr.gouv.vitam.query.exception.InvalidUuidOperationException;
import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.UUID;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * @author "Frederic Bregier"
 *
 */
@SuppressWarnings("serial")
public abstract class VitamType {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(VitamType.class);
    
    public static final String ID = "_id";
    JsonDocument document = null;
    String id = null;
    JsonObject values = JsonObject.empty();
    
    public VitamType() {
    }

    public final void setRoot() throws InvalidUuidOperationException {
        if (id == null) {
            id = new UUID().toString();
        }
        GlobalDatas.ROOTS.add(id);
    }
    
    public final void setNewId() {
    	id = new UUID().toString();
    }
    public final void setId(String id) {
    	this.id = id;
    }
    public String getId() {
        return id;
    }
    /**
     * Load from a JSON String
     * @param json
     */
    public final void load(String json) {
    	
        this.values.putAll((BSONObject) JSON.parse(json));
        this.getAfterLoad();
    }
    
    /**
     * To be called after any automatic load or loadFromJson to
     * update HashMap values.
     */
    public void getAfterLoad(){
    }
    /**
     * To be called before any collection.insert() or update if
     * HashMap values is changed.
     */
    public void putBeforeSave() {
    }
    /**
     * Save the object. Implementation should call putBeforeSave before the real save operation
     * @param dbvitam
     */
    public abstract void save(MongoDbAccess dbvitam);
    /**
     * try to update the object if necessary (difference from the current value in the database)
     * @param dbvitam
     * @return True if the object does not need any extra save operation
     */
    protected abstract boolean updated(MongoDbAccess dbvitam);
    /**
     * load the object from the database, ignoring any previous data, except ID
     * @param dbvitam
     */
    public abstract void load(MongoDbAccess dbvitam);
    /**
     * Save the document if new, update it (keeping non set fields, replacing set fields)
     * @param collection
     */
    protected final void updateOrSave(VitamCollection collection) {
        String id = (String) this.get(ID);
        if (id == null) {
            setNewId();
            collection.collection.save(this);
        } else {
            BasicDBObject upd = new BasicDBObject(this);
            upd.removeField(ID);
            collection.collection.update(new BasicDBObject(ID, id), new BasicDBObject("$set", upd));
        }
    }
    /**
     * Delete the current object
     * @param collection
     */
    public final void delete(VitamCollection collection) {
        collection.collection.remove(new BasicDBObject(ID, this.get(ID)));
    }
    /**
     * Update the item (must be saved before) using the update part
     * @param collection
     * @param update
     */
    protected final void update(VitamCollection collection, DBObject update) {
        try {
            collection.collection.update(new BasicDBObject(ID, this.get(ID)), update);
        } catch (MongoException e) {
            LOGGER.error("Exception for " + update, e);
            throw e;
        }
    }
    
    public String toStringDirect() {
        return super.toString();
    }
    public String toString() {
        return this.getClass().getSimpleName()+": "+super.toString();
    }
    public String toStringDebug() {
        return this.getClass().getSimpleName()+": "+this.get(ID);
    }

}
