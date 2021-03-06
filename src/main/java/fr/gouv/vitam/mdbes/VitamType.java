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
package fr.gouv.vitam.mdbes;

import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

import fr.gouv.vitam.mdbes.MongoDbAccess.VitamCollection;
import fr.gouv.vitam.query.GlobalDatas;
import fr.gouv.vitam.utils.UUID;
import fr.gouv.vitam.utils.exception.InvalidUuidOperationException;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * The default Vitam Type object to be stored in the database (MongoDb/ElasticSearch mode)
 *
 * @author "Frederic Bregier"
 *
 */
public abstract class VitamType extends BasicDBObject {
    private static final long serialVersionUID = 8051576404383272453L;

    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(VitamType.class);

    /**
     * Default ID field name
     */
    public static final String ID = "_id";

    /**
     * Empty constructor
     */
    public VitamType() {
    }

    /**
     * This (Domain) is a root
     *
     * @throws InvalidUuidOperationException
     */
    public final void setRoot() throws InvalidUuidOperationException {
        String id = (String) this.get(ID);
        if (id == null) {
            id = new UUID().toString();
        }
        GlobalDatas.ROOTS.add(id);
    }

    /**
     * Create a new ID
     */
    public final void setNewId() {
        append(ID, new UUID().toString());
    }

    /**
     * Set a new ID
     *
     * @param id
     */
    public final void setId(final String id) {
        append(ID, id);
    }

    /**
     *
     * @return the ID
     */
    public String getId() {
        return this.getString(ID);
    }

    /**
     * Load from a JSON String
     *
     * @param json
     */
    public final void load(final String json) {
        this.putAll((BSONObject) JSON.parse(json));
        getAfterLoad();
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
    public abstract void save(MongoDbAccess dbvitam);

    /**
     * try to update the object if necessary (difference from the current value in the database)
     *
     * @param dbvitam
     * @return True if the object does not need any extra save operation
     */
    protected abstract boolean updated(MongoDbAccess dbvitam);

    /**
     * load the object from the database, ignoring any previous data, except ID
     *
     * @param dbvitam
     * @return True if the object is loaded
     */
    public abstract boolean load(MongoDbAccess dbvitam);

    /**
     * Save the document if new, update it (keeping non set fields, replacing set fields)
     *
     * @param collection
     */
    protected final void updateOrSave(final VitamCollection collection) {
        final String id = (String) this.get(ID);
        if (id == null) {
            setNewId();
            collection.collection.save(this);
        } else {
            final BasicDBObject upd = new BasicDBObject(this);
            upd.removeField(ID);
            collection.collection.update(new BasicDBObject(ID, id), new BasicDBObject("$set", upd));
        }
    }
    
    /**
     * Force the save (insert) of this document (no putBeforeSave done)
     * @param collection
     */
    protected final void forceSave(final VitamCollection collection) {
        final String id = (String) this.get(ID);
        if (id == null) {
            setNewId();
        }
        collection.collection.save(this);
    }
    /**
     * Delete the current object
     *
     * @param collection
     */
    public final void delete(final VitamCollection collection) {
        collection.collection.remove(new BasicDBObject(ID, this.get(ID)));
    }

    /**
     * Update the item (must be saved before) using the update part
     *
     * @param collection
     * @param update
     */
    protected final void update(final VitamCollection collection, final DBObject update) {
        try {
            collection.collection.update(new BasicDBObject(ID, this.get(ID)), update);
        } catch (final MongoException e) {
            LOGGER.error("Exception for " + update, e);
            throw e;
        }
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
        return this.getClass().getSimpleName() + ": " + this.get(ID);
    }

}
