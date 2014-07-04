/**
   This file is part of POC MongoDB ElasticSearch Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   POC MongoDB ElasticSearch is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with POC MongoDB ElasticSearch .  If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.mdbes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BSONObject;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import fr.gouv.vitam.query.exception.InvalidUuidOperationException;
import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * MongoDb Access base class
 * @author "Frederic Bregier"
 *
 */
public class MongoDbAccess {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(MongoDbAccess.class);
    
    private DB db = null;
    private DB dbadmin = null;
    private VitamCollection[]collections = null;
    protected VitamCollection domains = null;
    protected VitamCollection daips = null;
    protected VitamCollection paips = null;
    protected VitamCollection saips = null;
    protected VitamCollection duarefs = null;
    protected VitamCollection requests = null;
    private ElasticSearchAccess es = null;
    
    private static enum LinkType {
        /**
         * Link N-N
         */
        SymLinkNN,
        /**
         * Link N-
         */
        AsymLinkN,
        /**
         * Link 1-N
         */
        SymLink1N,
        /**
         * Link N-1
         */
        SymLinkN1,
        /**
         * Link 1-1
         */
        SymLink11,
        /**
         * Link 1-
         */
        AsymLink1,
        /**
         * False Link (N)-N
         */
        SymLink_N_N
    }
    
    protected static enum VitamCollections{
        Cdomain(Domain.class), Cdaip(DAip.class), 
        Cpaip(PAip.class), Csaip(SAip.class), Cdua(DuaRef.class), 
        Crequests(ResultCached.class);
        
        @SuppressWarnings("rawtypes")
        private Class clasz;
        private String name;
        private int rank;
        private DBCollection collection;
        @SuppressWarnings("rawtypes")
        private VitamCollections(Class clasz) {
            this.clasz = clasz;
            this.name = clasz.getSimpleName();
            this.rank = ordinal();
        }
        protected String getName() {
        	return name;
        }
        protected DBCollection getCollection() {
        	return collection;
        }
    }
    
    protected static class VitamCollection{
        
        private VitamCollections coll;
        protected DBCollection collection;
        
        protected VitamCollection(DB db, VitamCollections coll, boolean recreate) {
            this.coll = coll;
            this.collection = db.getCollection(coll.name);
            this.collection.setObjectClass(coll.clasz);
            if (recreate) {
                //this.collection.dropIndexes();
                this.collection.createIndex(new BasicDBObject(VitamType.ID, "hashed"));
                //db.command(new BasicDBObject("collMod", coll.name).append("usePowerOf2Sizes", true));
            }
            this.coll.collection = this.collection;
        }
        protected DBCollection getCollection() {
        	return collection;
        }
    }
    
    // Structure Access
    protected static enum VitamLinks{
        /**
         * Domain to DAip N-N link. This link is symmetric.
         */
        Domain2DAip(VitamCollections.Cdomain, LinkType.SymLinkNN, "_daips", VitamCollections.Cdaip, "_doms"),
        /**
         * Daip to Daip N-N link but asymmetric where only childs reference their fathers (so only "_up" link)
         */
        DAip2DAip(VitamCollections.Cdaip, LinkType.SymLink_N_N, "_down_unused", VitamCollections.Cdaip, "_up"),
        /**
         * Daip to Paip 1-N link. This link is symmetric.
         */
        DAip2PAip(VitamCollections.Cdaip, LinkType.SymLink1N, "_paip", VitamCollections.Cpaip, "_up"),
        /**
         * Paip to Saip 1-1 link. Ths link is symmetric.
         */
        PAip2SAip(VitamCollections.Cpaip, LinkType.SymLink11, "_saip", VitamCollections.Csaip, "_paip"),
        /**
         * Daip to Dua 1-N link but asymmetric where only Daip reference its Dua (so only "_dua" link)
         */
        DAip2Dua(VitamCollections.Cdaip, LinkType.AsymLink1, "_dua", VitamCollections.Cdua),
        /**
         * Daip to Dua N-N link but asymmetric where only Daip reference its Dua(s) (so only "_dua" link)
         */
        PAip2Dua(VitamCollections.Cpaip, LinkType.AsymLinkN, "_duas", VitamCollections.Cdua);
        
        protected VitamCollections col1;
        protected LinkType type;
        protected String field1to2;
        protected VitamCollections col2;
        protected String field2to1;

        
        /**
         * @param col1
         * @param type
         * @param field1to2
         * @param col2
         * @param field2to1
         */
        private VitamLinks(VitamCollections col1, LinkType type, String field1to2, VitamCollections col2, String field2to1) {
            this.col1 = col1;
            this.type = type;
            this.field1to2 = field1to2;
            this.col2 = col2;
            this.field2to1 = field2to1;
        }

        /**
         * @param clasz1
         * @param type
         * @param field1to2
         * @param clasz2
         */
        private VitamLinks(VitamCollections col1, LinkType type, String field1to2, VitamCollections col2) {
            this.col1 = col1;
            this.type = type;
            this.field1to2 = field1to2;
            this.col2 = col2;
        }
        
    }
    /**
     * 
     * @param mongoClient the current valid MongoClient to use as connector to the database
     * @param dbname the MongoDB database name
     * @param esname the ElasticSearch name
     * @param unicast the unicast addresses for ElasticSearch
     * @param recreate shall we recreate the index
     * @throws InvalidUuidOperationException
     */
    public MongoDbAccess(MongoClient mongoClient, String dbname, String esname, String unicast, boolean recreate) throws InvalidUuidOperationException {
        db = mongoClient.getDB(dbname);
        dbadmin = mongoClient.getDB("admin");
        // Authenticate - optional
        // boolean auth = db.authenticate("foo", "bar");
        
        collections = new VitamCollection[VitamCollections.values().length];
        // get a collection object to work with
        domains = collections[VitamCollections.Cdomain.rank] = new VitamCollection(db, VitamCollections.Cdomain, recreate);
        daips = collections[VitamCollections.Cdaip.rank] = new VitamCollection(db, VitamCollections.Cdaip, recreate);
        paips = collections[VitamCollections.Cpaip.rank] = new VitamCollection(db, VitamCollections.Cpaip, recreate);
        saips = collections[VitamCollections.Csaip.rank] = new VitamCollection(db, VitamCollections.Csaip, recreate);
        duarefs = collections[VitamCollections.Cdua.rank] = new VitamCollection(db, VitamCollections.Cdua, recreate);
        requests = collections[VitamCollections.Crequests.rank] = new VitamCollection(db, VitamCollections.Crequests, recreate);
        DBCursor cursor = domains.collection.find();
        for (DBObject dbObject : cursor) {
            Domain dom = (Domain) dbObject;
            dom.setRoot();
        }
        // elasticsearch index
        LOGGER.info("ES on cluster name: "+esname+":"+unicast);
        es = new ElasticSearchAccess(esname, unicast, GlobalDatas.localNetworkAddress);
    }
    /**
     * Drop all data and index from MongoDB and ElasticSearch
     * @param model
     */
    public final void reset(String model) {
		for (int i = 0; i < collections.length; i++) {
			collections[i].collection.drop();
		}
		es.deleteIndex(GlobalDatas.INDEXNAME);
		es.addIndex(GlobalDatas.INDEXNAME, model);
		ensureIndex();
    }
    /**
     * Update the Index for a new model
     * @param model
     */
    public void updateEsIndex(String model) {
		es.addIndex(GlobalDatas.INDEXNAME, model);
    }
    /**
     * Close database access (in particular for ElasticSearch)
     */
    public final void close() {
        es.close();
    }
    /**
     * Ensure that all MongoDB database schema are indexed
     */
    public void ensureIndex() {
        for (int i = 0; i < collections.length; i++) {
            collections[i].collection.createIndex(new BasicDBObject(VitamType.ID, "hashed"));
        }
        Domain.addIndexes(this);
        DAip.addIndexes(this);
        PAip.addIndexes(this);
        SAip.addIndexes(this);
        DuaRef.addIndexes(this);
        ResultCached.addIndexes(this);
    }
    /**
     * Remove temporarily the MongoDB Index (import optimization?)
     */
    public void removeIndexBeforeImport() {
    	try {
			daips.collection.dropIndex(new BasicDBObject(VitamLinks.DAip2DAip.field2to1, 1));
			daips.collection.dropIndex(new BasicDBObject(VitamLinks.Domain2DAip.field2to1, 1));
			daips.collection.dropIndex(new BasicDBObject(DAip.DAIPDEPTHS, 1));
		} catch (Exception e) {
			LOGGER.error("Error while removing indexes before import", e);
		}
    }
    /**
     * Reset MongoDB Index (import optimization?)
     */
    public void resetIndexAfterImport() {
		LOGGER.info("Rebuild indexes");
		daips.collection.createIndex(new BasicDBObject(VitamLinks.DAip2DAip.field2to1, 1));
		daips.collection.createIndex(new BasicDBObject(VitamLinks.Domain2DAip.field2to1, 1));
		daips.collection.createIndex(new BasicDBObject(DAip.DAIPDEPTHS, 1));
    }
    public String toString() {
    	StringBuilder builder = new StringBuilder();
    	// get a list of the collections in this database and print them out
    	Set<String> collectionNames = db.getCollectionNames();
		for (String s : collectionNames) {
			builder.append(s);
			builder.append('\n');
		}
		for (VitamCollection coll : collections) {
			List<DBObject> list = coll.collection.getIndexInfo();
			for (DBObject dbObject : list) {
				builder.append(coll.coll.name);
				builder.append(' ');
				builder.append(dbObject);
				builder.append('\n');
			}
		}
		return builder.toString();
    }
    /**
     * 
     * @return the current number of DAip
     */
    public long getDaipSize() {
    	return daips.collection.count();
    }
    /**
     * 
     * @return the current number of PAip
     */
    public long getPaipSize() {
    	return paips.collection.count();
    }
    /**
     * Force flush on disk (MongoDB): should not be used
     */
    public void flushOnDisk() {
        dbadmin.command(new BasicDBObject("fsync", 1).append("async", true));
    }
    /**
     * 
     * @param collection
     * @param ref
     * @return a VitamType generic object from ID ref value
     */
    public final VitamType loadFromObjectId(VitamCollection collection, String ref) {
        BasicDBObject obj = new BasicDBObject(VitamType.ID, ref);
        return (VitamType) collection.collection.findOne(obj);
    }
    
    /**
     * Load a BSONObject into VitamType
     * @param obj
     * @param coll
     * @return the VitamType casted object
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public final VitamType loadFromBSONObject(BSONObject obj, VitamCollections coll) throws InstantiationException, IllegalAccessException {
        VitamType vt = (VitamType)coll.clasz.newInstance();
        vt.putAll(obj);
        vt.getAfterLoad();
        return vt;
    }

    /**
     * 
     * @param collection
     * @param field
     * @param ref
     * @return the VitamType casted object
     */
    public final VitamType fineOne(VitamCollections col, String field, String ref) {
        BasicDBObject obj = new BasicDBObject(field, ref);
        VitamType vitobj = (VitamType) col.collection.findOne(obj);
        if (vitobj == null) {
            return null;
        } else {
            vitobj.getAfterLoad();
        }
        return vitobj;
    }

    /**
     * Find the corresponding id in col collection if it exists
     * @param col
     * @param id
     * @return the VitamType casted object
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public final VitamType findOne(VitamCollections col, String id) throws InstantiationException, IllegalAccessException {
        if (id == null || id.length() == 0) {
            return null;
        }
        BasicDBObject query = new BasicDBObject(VitamType.ID, id);
        VitamType vitobj = (VitamType) col.collection.findOne(query);
        if (vitobj == null) {
            return null;
        } else {
            vitobj.getAfterLoad();
        }
        return vitobj;
    }

    /**
     * 
     * @param col
     * @param id
     * @return True if one VitamType object exists with this id
     */
    public final boolean exists(VitamCollections col, String id) {
        if (id == null || id.length() == 0) {
            return false;
        }
        BasicDBObject query = new BasicDBObject(VitamType.ID, id);
        return col.collection.count(query) > 0;
    }

    /**
     * 
     * @param collection domain of request
     * @param condition where condition
     * @param idProjection select condition
     * @return the DbCursor on the find request based on the given collection
     */
    public final DBCursor find(VitamCollection collection, BasicDBObject condition, BasicDBObject idProjection) {
        return collection.collection.find(condition, idProjection);
    }

    /**
     * 
     * @param indexName
     * @param type
     * @param currentNodes current parent nodes
     * @param subdepth (ignored)
     * @param condition
     * @param filterCond
     * @return the ResultCached associated with this request. Note that the exact depth is not checked, so it must be checked after (using checkAncestor method)
     */
    public final ResultCached getSubDepth(String indexName, String type, Collection<String> currentNodes, int subdepth, QueryBuilder condition, FilterBuilder filterCond) {
        return es.getSubDepth(indexName, type, currentNodes.toArray(new String[0]), subdepth, condition, filterCond);
    }
    /**
     * Add indexes to ES model
     * @param indexes
     * @param model
     */
    public final void addEsEntryIndex(Map<String, String> indexes, String model) {
        addEsEntryIndex(GlobalDatas.BLOCKING, indexes, model);
    }
    /**
     * Add indexes to ES model
     * @param indexes
     * @param model
     */
    public final void addEsEntryIndex(boolean blocking, Map<String, String> indexes, String model) {
        if (blocking) {
            es.addEntryIndexesBlocking(GlobalDatas.INDEXNAME, model, indexes);
        } else {
            es.addEntryIndexes(GlobalDatas.INDEXNAME, model, indexes);
        }
    }
    /**
     * Add a Link according to relation defined, where the relation is defined in obj1->obj2 way by default (even if symmetric)
     * @param obj1
     * @param relation
     * @param obj2
     * @return a {@link DBObject} that hold a possible update part (may be null)
     */
    public final DBObject addLink(VitamType obj1, VitamLinks relation, VitamType obj2) {
        switch (relation.type) {
            case AsymLink1:
                MongoDbAccess.addAsymmetricLink(obj1, relation.field1to2, obj2);
                break;
            case SymLink11:
                MongoDbAccess.addAsymmetricLink(obj1, relation.field1to2, obj2);
                return MongoDbAccess.addAsymmetricLinkUpdate(obj2, relation.field2to1, obj1);
            case AsymLinkN:
                MongoDbAccess.addAsymmetricLinkset(obj1, relation.field1to2, obj2, false);
                break;
            case SymLink1N:
                return MongoDbAccess.addSymmetricLink(obj1, relation.field1to2, 
                        obj2, relation.field2to1);
            case SymLinkN1:
                return MongoDbAccess.addReverseSymmetricLink(obj1, relation.field1to2, 
                        obj2, relation.field2to1);
            case SymLinkNN:
                return MongoDbAccess.addSymmetricLinkset(obj1, relation.field1to2, 
                        obj2, relation.field2to1);
            case SymLink_N_N:
                return addAsymmetricLinkset(obj2, relation.field2to1, obj1, true);
            default:
                break;
        }
        return null;
    }
    /**
     * Update the link
     * @param obj1
     * @param vtReloaded
     * @param relation
     * @param src
     * @return the update part
     */
    public final BasicDBObject updateLink(VitamType obj1, VitamType vtReloaded, VitamLinks relation, boolean src) {
        //DBCollection coll = (src ? relation.col1.collection : relation.col2.collection);
        String fieldname = (src ? relation.field1to2 : relation.field2to1);
        //VitamType vt = (VitamType) coll.findOne(new BasicDBObject("_id", obj1.get("_id")));
        if (vtReloaded != null) {
            String srcOid = (String) vtReloaded.remove(fieldname);
            String targetOid = (String) obj1.get(fieldname);
            if (srcOid != null && targetOid != null) {
                if (targetOid.equals(srcOid)) {
                    srcOid = null;
                } else {
                    srcOid = targetOid;
                }
            } else if (targetOid != null) {
                srcOid = targetOid;
            } else if (srcOid != null) {
                obj1.put(fieldname, srcOid);
                srcOid = null;
            }
            if (srcOid != null) {
                // need to add $set
                return new BasicDBObject(fieldname, srcOid);
            }
        } else {
            // nothing since save will be done just after
        }
        return null;
    }
    /**
     * Update the links
     * @param obj1
     * @param vtReloaded
     * @param relation
     * @param src
     * @return the update part
     */
    public final BasicDBObject updateLinks(VitamType obj1, VitamType vtReloaded, VitamLinks relation, boolean src) {
        //DBCollection coll = (src ? relation.col1.collection : relation.col2.collection);
        String fieldname = (src ? relation.field1to2 : relation.field2to1);
        //VitamType vt = (VitamType) coll.findOne(new BasicDBObject("_id", obj1.get("_id")));
        if (vtReloaded != null) {
            @SuppressWarnings("unchecked")
            List<String> srcList = (List<String>) vtReloaded.remove(fieldname);
            @SuppressWarnings("unchecked")
            List<String> targetList = (List<String>) obj1.get(fieldname);
            if (srcList != null && targetList != null) {
                targetList.removeAll(srcList);
            } else if (targetList != null) {
                // srcList empty
            } else {
                // targetList empty
                obj1.put(fieldname, srcList);
            }
            if (targetList != null && !targetList.isEmpty()) {
                // need to add $addToSet
                return new BasicDBObject(fieldname, new BasicDBObject("$each", targetList));
            }
        } else {
            // nothing since save will be done just after, except checking array exists
            if (! obj1.containsField(fieldname)) {
                obj1.put(fieldname, new ArrayList<>());
            }
        }
        return null;
    }

    public final void updateLinksToFile(VitamType obj1, VitamLinks relation, boolean src) {
        String fieldname = (src ? relation.field1to2 : relation.field2to1);
        // nothing since save will be done just after, except checking array exists
        if (! obj1.containsField(fieldname)) {
            obj1.put(fieldname, new ArrayList<>());
        }
    }
    
    /**
     * Add an asymmetric relation (n-1) between Obj1 and Obj2 
     * @param obj1
     * @param obj1ToObj2
     * @param obj2
     * @param obj2ToObj1
     * @return a {@link DBObject} for update
     */
    protected final static DBObject addReverseSymmetricLink(VitamType obj1, String obj1ToObj2, 
            VitamType obj2, String obj2ToObj1) {
        addAsymmetricLinkset(obj1, obj1ToObj2, obj2, false);
        return addAsymmetricLinkUpdate(obj2, obj2ToObj1, obj1);
    }

    /**
     * Add an asymmetric relation (1-n) between Obj1 and Obj2 
     * @param obj1
     * @param obj1ToObj2
     * @param obj2
     * @param obj2ToObj1
     * @return a {@link DBObject} for update
     */
    protected final static DBObject addSymmetricLink(VitamType obj1, String obj1ToObj2, 
            VitamType obj2, String obj2ToObj1) {
        addAsymmetricLink(obj1, obj1ToObj2, obj2);
        return addAsymmetricLinkset(obj2, obj2ToObj1, obj1, true);
    }

    /**
     * Add a symmetric relation (n-n) between Obj1 and Obj2 
     * @param obj1
     * @param obj1ToObj2
     * @param obj2
     * @param obj2ToObj1
     * @return a {@link DBObject} for update
     */
    protected final static DBObject addSymmetricLinkset(VitamType obj1, String obj1ToObj2, 
            VitamType obj2, String obj2ToObj1) {
        addAsymmetricLinkset(obj1, obj1ToObj2, obj2, false);
        return addAsymmetricLinkset(obj2, obj2ToObj1, obj1, true);
    }

    /**
     * Add a single relation (1) from Obj1 to Obj2 
     * @param obj1
     * @param obj1ToObj2
     * @param obj2
     */
    protected final static void addAsymmetricLink(VitamType obj1, String obj1ToObj2, VitamType obj2) {
        String refChild = (String) obj2.get(VitamType.ID);
        obj1.put(obj1ToObj2, refChild);
    }
    /**
     * Add a single relation (1) from Obj1 to Obj2 in update mode 
     * @param db
     * @param obj1
     * @param obj1ToObj2
     * @param obj2
     */
    protected final static DBObject addAsymmetricLinkUpdate(VitamType obj1, String obj1ToObj2, VitamType obj2) {
        String refChild = (String) obj2.get(VitamType.ID);
        if (obj1.containsField(obj1ToObj2)) {
            if (obj1.get(obj1ToObj2).equals(refChild)) {
                return null;
            }
        }
        obj1.put(obj1ToObj2, refChild);
        return new BasicDBObject("$set", new BasicDBObject(obj1ToObj2, refChild));
    }
    /**
     * Add a one way relation (n) from Obj1 to Obj2, with no Save
     * @param obj1
     * @param obj1ToObj2
     * @param obj2
     * @param toUpdate
     * @return true if the link is updated
     */
    public final static boolean addAsymmetricLinksetNoSave(VitamType obj1, String obj1ToObj2, VitamType obj2, boolean toUpdate) {
        @SuppressWarnings("unchecked")
        ArrayList<String> relation12 = (ArrayList<String>) obj1.get(obj1ToObj2);
        String oid2 = (String) obj2.get(VitamType.ID);
        if (relation12 == null) {
            relation12 = new ArrayList<String>();
        }
        if (relation12.contains(oid2)) {
            return false;
        }
        relation12.add(oid2);
        obj1.put(obj1ToObj2, relation12);
        return true;
    }
    /**
     * Add a one way relation (n) from Obj1 to Obj2 
     * @param obj1
     * @param obj1ToObj2
     * @param obj2
     * @param toUpdate True if this element will be updated through $addToSet only
     * @return a {@link DBObject} for update
     */
    protected final static DBObject addAsymmetricLinkset(VitamType obj1, String obj1ToObj2, VitamType obj2, boolean toUpdate) {
        @SuppressWarnings("unchecked")
        ArrayList<String> relation12 = (ArrayList<String>) obj1.get(obj1ToObj2);
        String oid2 = (String) obj2.get(VitamType.ID);
        if (relation12 == null) {
            if (toUpdate) {
                return new BasicDBObject("$addToSet", new BasicDBObject(obj1ToObj2, oid2));
            }
            relation12 = new ArrayList<String>();
        }
        if (relation12.contains(oid2)) {
            return null;
        }
        if (toUpdate) {
            return new BasicDBObject("$addToSet", new BasicDBObject(obj1ToObj2, oid2));
        } else {
            relation12.add(oid2);
            obj1.put(obj1ToObj2, relation12);
            return null;
        }
    }
    

}
