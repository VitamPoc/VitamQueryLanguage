/**
 * This file is part of POC MongoDB ElasticSearch Project.
 *
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import fr.gouv.vitam.mdbes.MongoDbAccess.VitamCollection;
import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * Result (potentially cached) object
 *
 * @author "Frederic Bregier"
 *
 */
public class CopyOfResultMongodb extends ResultAbstract implements DBObject {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(CopyOfResultMongodb.class);
    /**
     * TTL
     */
    public static final String TTL = "__ttl";
    /**
     * ttl date
     */
    private Date ttl = getNewTtl();
    /**
     * 
     */
    protected BasicDBObject obj = new BasicDBObject();
    
    private static final Date getNewTtl() {
        return new Date(System.currentTimeMillis()+GlobalDatas.TTLMS);
    }
    /**
     *
     */
    public CopyOfResultMongodb() {

    }

    /**
     * @param collection
     */
    public CopyOfResultMongodb(final Collection<String> collection) {
        currentDaip.addAll(collection);
        updateMinMax();
        // Path list so as loaded (never cached)
        loaded = true;
        putBeforeSave();
    }
    /**
     * Put from argument
     * @param from
     */
    public void putFrom(final ResultInterface from) {
        obj.putAll((BSONObject) from);
        loaded = true;
        getAfterLoad();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void getAfterLoad() {
        if (obj.containsField(CURRENTDAIP)) {
            final Object obj2 = obj.get(CURRENTDAIP);
            final Set<String> vtset = new HashSet<String>();
            if (obj2 instanceof BasicDBList) {
                for (Object string : (BasicDBList) obj2) {
                    vtset.add((String) string);
                }
            } else {
                vtset.addAll((Set<String>) obj2);
            }
            currentDaip.clear();
            currentDaip.addAll(vtset);
        }
        minLevel = obj.getInt(MINLEVEL, 0);
        maxLevel = obj.getInt(MAXLEVEL, 0);
        nbSubNodes = obj.getLong(NBSUBNODES, -1);
        ttl = obj.getDate(TTL, getNewTtl());
    }

    @Override
    public void putBeforeSave() {
        ttl = getNewTtl();
        if (!currentDaip.isEmpty()) {
            obj.put(CURRENTDAIP, currentDaip);
        }
        obj.put(MINLEVEL, minLevel);
        obj.put(MAXLEVEL, maxLevel);
        obj.put(NBSUBNODES, nbSubNodes);
        obj.put(TTL, ttl);
    }

    @SuppressWarnings("unchecked")
    protected boolean updated(final MongoDbAccess dbvitam) {
        if (getId() == null) {
            return false;
        }
        final CopyOfResultMongodb vt = (CopyOfResultMongodb) dbvitam.requests.collection.findOne(getId());
        if (vt != null) {
            final List<DBObject> list = new ArrayList<DBObject>();
            final Object obj = vt.obj.get(CURRENTDAIP);
            final Set<String> vtset = new HashSet<String>();
            if (obj instanceof BasicDBList) {
                for (Object string : (BasicDBList) obj) {
                    vtset.add((String) string);
                }
            } else {
                vtset.addAll((Set<String>) obj);
            }
            if (! vtset.isEmpty()) {
                final Set<String> newset = new HashSet<String>(currentDaip);
                newset.removeAll(vtset);
                if (!newset.isEmpty()) {
                    list.add(new BasicDBObject(CURRENTDAIP, new BasicDBObject("$each", newset)));
                }
            }
            if (!list.isEmpty()) {
                final BasicDBObject updset = new BasicDBObject();
                for (final DBObject dbObject : list) {
                    updset.putAll(dbObject);
                }
                final BasicDBObject upd = new BasicDBObject();
                upd.append(MINLEVEL, minLevel);
                upd.append(MAXLEVEL, maxLevel);
                upd.append(NBSUBNODES, nbSubNodes);
                upd.append(TTL, ttl);
                final BasicDBObject update = new BasicDBObject("$addToSet", updset).
                        append("$set", upd);
                dbvitam.requests.collection.update(new BasicDBObject(ID, this.obj.get(ID)), update);
            }
            if (GlobalDatas.PRINT_REQUEST) {
                LOGGER.warn("UPDATE: "+this);
            }
        } else if (obj.containsField(ID)) {
            // not in DB but got already an ID => Save it
            if (GlobalDatas.PRINT_REQUEST) {
                LOGGER.warn("SAVE: "+this);
            }
            this.forceSave(dbvitam.requests);
            return true;
        }
        return false;
    }
    
    /**
     * Force the save (insert) of this document (no putBeforeSave done)
     * @param collection
     */
    protected final void forceSave(final VitamCollection collection) {
        final String id = (String) obj.get(ID);
        if (id == null) {
            // shall not be
            return;
        }
        collection.collection.save(obj);
    }
    /**
     * Create a new ID
     */
    public final void setNewId() {
        // not allowed
    }
    /**
     * Save the document if new, update it (keeping non set fields, replacing set fields)
     *
     * @param collection
     */
    protected final void updateOrSave(final VitamCollection collection) {
        final String id = (String) obj.get(ID);
        if (id != null) {
            final BasicDBObject upd = new BasicDBObject(obj);
            upd.removeField(ID);
            collection.collection.update(new BasicDBObject(ID, id), new BasicDBObject("$set", upd));
        }
    }
    
    @Override
    public void save(final MongoDbAccess dbvitam) {
        putBeforeSave();
        if (updated(dbvitam)) {
            loaded = true;
            return;
        }
        updateOrSave(dbvitam.requests);
        loaded = true;
    }
    /**
     * Update the TTL for this
     * @param dbvitam
     */
    public void updateTtl(final MongoDbAccess dbvitam) {
        final String id = (String) obj.get(ID);
        if (id == null) {
            return;
        }
        ttl = getNewTtl();
        dbvitam.requests.collection.update(new BasicDBObject(ID, id), 
                new BasicDBObject().append("$set", new BasicDBObject(TTL, ttl)));
    }
    /**
     * Set a new ID
     *
     * @param id
     */
    public final void setId(final MongoDbAccess dbvitam, final String id) {
        if (id == null) {
            return;
        }
        obj.append(ID, id);
    }

    /**
     *
     * @return the ID
     */
    public String getId() {
        return obj.getString(ID);
    }
    @Override
    public Object put(String key, Object v) {
        return obj.put(key, v);
    }
    @Override
    public void putAll(BSONObject o) {
        obj.putAll(o);
    }
    @Override
    public void putAll(@SuppressWarnings("rawtypes") Map m) {
        obj.putAll(m);
    }
    @Override
    public Object get(String key) {
        return obj.get(key);
    }
    @Override
    public Map<?, ?> toMap() {
        return obj.toMap();
    }
    @Override
    public Object removeField(String key) {
        return obj.removeField(key);
    }
    @Override
    public boolean containsKey(String s) {
        return obj.containsField(s);
    }
    @Override
    public boolean containsField(String s) {
        return obj.containsField(s);
    }
    @Override
    public Set<String> keySet() {
        return obj.keySet();
    }
    @Override
    public void markAsPartialObject() {
        obj.markAsPartialObject();
    }
    @Override
    public boolean isPartialObject() {
        return obj.isPartialObject();
    }

    protected static void addIndexes(final MongoDbAccess mongoDbAccess) {
        mongoDbAccess.requests.collection.createIndex(new BasicDBObject(TTL, 1), new BasicDBObject("expireAfterSeconds", 0));
    }

}
