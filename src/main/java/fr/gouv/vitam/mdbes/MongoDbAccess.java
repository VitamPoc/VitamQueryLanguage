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

import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BSONObject;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.exception.InvalidUuidOperationException;
import fr.gouv.vitam.utils.FileUtil;
import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.UUID;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * MongoDb Access base class
 *
 * @author "Frederic Bregier"
 *
 */
public class MongoDbAccess {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(MongoDbAccess.class);

    private DB db = null;
    private DB dbadmin = null;
    private VitamCollection[] collections = null;
    protected VitamCollection domains = null;
    protected VitamCollection daips = null;
    protected VitamCollection paips = null;
    protected VitamCollection saips = null;
    protected VitamCollection duarefs = null;
    protected VitamCollection requests = null;
    private ElasticSearchAccess es = null;
    protected CouchbaseAccess cba = null;
    protected RedisAccess ra = null;
    protected MessageDigest md;
    
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

    protected static enum VitamCollections {
        Cdomain(Domain.class), Cdaip(DAip.class), Cpaip(PAip.class), Csaip(SAip.class), Cdua(DuaRef.class), 
        Crequests(ResultMongodb.class);

        @SuppressWarnings("rawtypes")
        private Class clasz;
        private String name;
        private int rank;
        private DBCollection collection = null;

        @SuppressWarnings("rawtypes")
        private VitamCollections(final Class clasz) {
            this.clasz = clasz;
            name = clasz.getSimpleName();
            rank = ordinal();
        }

        protected String getName() {
            return name;
        }

        protected DBCollection getCollection() {
            return collection;
        }
    }

    protected static class VitamCollection {

        private final VitamCollections coll;
        protected DBCollection collection;

        protected VitamCollection(final DB db, final VitamCollections coll, final boolean recreate) {
            this.coll = coll;
            collection = db.getCollection(coll.name);
            collection.setObjectClass(coll.clasz);
            if (recreate) {
                // this.collection.dropIndexes();
                collection.createIndex(new BasicDBObject(VitamType.ID, "hashed"));
                // db.command(new BasicDBObject("collMod", coll.name).append("usePowerOf2Sizes", true));
            }
            this.coll.collection = collection;
        }

        protected DBCollection getCollection() {
            return collection;
        }
    }

    // Structure Access
    protected static enum VitamLinks {
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
         * Paip to Dua N-N link but asymmetric where only Paip reference its Dua(s) (so only "_duas" link)
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
        private VitamLinks(final VitamCollections col1, final LinkType type, final String field1to2, final VitamCollections col2,
                final String field2to1) {
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
        private VitamLinks(final VitamCollections col1, final LinkType type, final String field1to2, final VitamCollections col2) {
            this.col1 = col1;
            this.type = type;
            this.field1to2 = field1to2;
            this.col2 = col2;
        }

    }

    /**
     *
     * @param mongoClient
     *            the current valid MongoClient to use as connector to the database
     * @param dbname
     *            the MongoDB database name
     * @param esname
     *            the ElasticSearch name
     * @param unicast
     *            the unicast addresses for ElasticSearch
     * @param recreate
     *            shall we recreate the index
     * @throws InvalidUuidOperationException
     */
    @SuppressWarnings("unused")
    public MongoDbAccess(final MongoClient mongoClient, final String dbname, final String esname, final String unicast,
            final boolean recreate) throws InvalidUuidOperationException {
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
        if (GlobalDatas.USECOUCHBASE || GlobalDatas.USELRUCACHE || GlobalDatas.USEREDIS) {
            requests = null;
            collections[VitamCollections.Crequests.rank] = null;
        } else {
            requests = collections[VitamCollections.Crequests.rank] = new VitamCollection(db, VitamCollections.Crequests, recreate);
        }
        final DBCursor cursor = domains.collection.find();
        for (final DBObject dbObject : cursor) {
            final Domain dom = (Domain) dbObject;
            dom.setRoot();
        }
        // elasticsearch index
        LOGGER.info("ES on cluster name: " + esname + ":" + unicast);
        es = new ElasticSearchAccess(esname, unicast, GlobalDatas.localNetworkAddress);
        try {
            md = MessageDigest.getInstance("SHA-512");
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error(e);
        }
        if (GlobalDatas.USEREDIS) {
            ra = new RedisAccess(unicast, 20);
        }
    }
    /**
     * Connect to Couchbase
     * @param hosts
     * @param bucket
     * @param password
     * @throws InvalidCreateOperationException
     */
    public void connectCouchbase(List<URI> hosts, String bucket, String password) throws InvalidCreateOperationException {
        if (GlobalDatas.USECOUCHBASE) {
            cba = new CouchbaseAccess(hosts, bucket, password);
        }
    }
    /**
     * 
     * @return the ES Cluster Name
     */
    public String getEsClusterName() {
        return es.getClusterName();
    }
    /**
     * 
     * @param tohash
     * @return the corresponding digest as a "false" String
     */
    public final String createDigest(final String tohash) {
        synchronized (md) {
            md.update(tohash.getBytes(FileUtil.UTF8));
            return UUID.toHex(md.digest());
        }
    }
    /**
     * Drop all data and index from MongoDB and ElasticSearch
     *
     * @param model
     */
    public final void reset(final String model) {
        for (int i = 0; i < collections.length; i++) {
            collections[i].collection.drop();
        }
        es.deleteIndex(GlobalDatas.INDEXNAME);
        es.addIndex(GlobalDatas.INDEXNAME, model);
        ensureIndex();
    }

    /**
     * Update the Index for a new model
     *
     * @param model
     */
    public void updateEsIndex(final String model) {
        es.addIndex(GlobalDatas.INDEXNAME, model);
    }

    /**
     * Close database access (ElasticSearch, Couchbase, Redis, ...)
     */
    public final void close() {
        es.close();
        if (cba != null) {
            cba.close();
        }
        if (ra != null) {
            ra.close();
        }
    }
    /**
     * To be called once only when closing the application
     */
    public final void closeFinal() {
        if (ra != null) {
            ra.finalClose();
        }
    }

    /**
     * Ensure that all MongoDB database schema are indexed
     */
    @SuppressWarnings("unused")
    public void ensureIndex() {
        for (int i = 0; i < collections.length; i++) {
            collections[i].collection.createIndex(new BasicDBObject(VitamType.ID, "hashed"));
        }
        Domain.addIndexes(this);
        DAip.addIndexes(this);
        PAip.addIndexes(this);
        SAip.addIndexes(this);
        DuaRef.addIndexes(this);
        if (!(GlobalDatas.USECOUCHBASE && GlobalDatas.USELRUCACHE && GlobalDatas.USEREDIS)) {
            ResultMongodb.addIndexes(this);
        }
    }

    /**
     * Remove temporarily the MongoDB Index (import optimization?)
     */
    public void removeIndexBeforeImport() {
        try {
            daips.collection.dropIndex(new BasicDBObject(VitamLinks.DAip2DAip.field2to1, 1));
            daips.collection.dropIndex(new BasicDBObject(VitamLinks.Domain2DAip.field2to1, 1));
            daips.collection.dropIndex(new BasicDBObject(DAip.DAIPDEPTHS, 1));
        } catch (final Exception e) {
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

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        // get a list of the collections in this database and print them out
        final Set<String> collectionNames = db.getCollectionNames();
        for (final String s : collectionNames) {
            builder.append(s);
            builder.append('\n');
        }
        for (final VitamCollection coll : collections) {
            final List<DBObject> list = coll.collection.getIndexInfo();
            for (final DBObject dbObject : list) {
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
     * 
     * @return the size of the Result Cache
     */
    public long getCacheSize() {
        if (GlobalDatas.USECOUCHBASE) {
            // Couchbase
            return cba.getCount();
        } else if (GlobalDatas.USELRUCACHE) {
            return ResultLRU.count();
        } else if (GlobalDatas.USEREDIS) {
            return ra.getCount();
        } else {
            return requests.collection.count();
        }
    }

    /**
     * Force flush on disk (MongoDB): should not be used
     */
    protected void flushOnDisk() {
        dbadmin.command(new BasicDBObject("fsync", 1).append("async", true));
    }

    /**
     *
     * @param collection
     * @param ref
     * @return a VitamType generic object from ID ref value
     */
    public final VitamType loadFromObjectId(final VitamCollection collection, final String ref) {
        return (VitamType) collection.collection.findOne(ref);
    }

    /**
     * Load a BSONObject into VitamType
     *
     * @param obj
     * @param coll
     * @return the VitamType casted object
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public final VitamType loadFromBSONObject(final BSONObject obj, final VitamCollections coll) throws InstantiationException,
    IllegalAccessException {
        final VitamType vt = (VitamType) coll.clasz.newInstance();
        vt.putAll(obj);
        vt.getAfterLoad();
        return vt;
    }

    /**
     *
     * @param col
     * @param collection
     * @param field
     * @param ref
     * @return the VitamType casted object
     */
    public final VitamType fineOne(final VitamCollections col, final String field, final String ref) {
        final BasicDBObject obj = new BasicDBObject(field, ref);
        final VitamType vitobj = (VitamType) col.collection.findOne(obj);
        if (vitobj == null) {
            return null;
        } else {
            vitobj.getAfterLoad();
        }
        return vitobj;
    }

    /**
     * Find the corresponding id in col collection if it exists
     *
     * @param col
     * @param id
     * @return the VitamType casted object
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public final VitamType findOne(final VitamCollections col, final String id) throws InstantiationException,
            IllegalAccessException {
        if (id == null || id.length() == 0) {
            return null;
        }
        final VitamType vitobj = (VitamType) col.collection.findOne(id);
        if (vitobj == null) {
            return null;
        } else {
            vitobj.getAfterLoad();
        }
        return vitobj;
    }
    
    private static BasicDBObject IDONLY = new BasicDBObject(VitamType.ID, 1);

    /**
     *
     * @param col
     * @param id
     * @return True if one VitamType object exists with this id
     */
    public final boolean exists(final VitamCollections col, final String id) {
        if (id == null || id.length() == 0) {
            return false;
        }
        if (col == VitamCollections.Crequests) {
            if (GlobalDatas.USECOUCHBASE) {
                // Couchbase
                return cba.exists(id);
            } else if (GlobalDatas.USELRUCACHE) {
                return ResultLRU.exists(id);
            } else if (GlobalDatas.USEREDIS) {
                return ra.exists(id);
            } else {
                String nid = createDigest(id);
                return col.collection.findOne(nid, IDONLY) != null;
            }
        }
        return col.collection.findOne(id, IDONLY) != null;
    }
    /**
    *
    * @param id
    * @return the ResultInterface if any (null else)
    */
    public final ResultInterface load(final String id) {
       if (id == null || id.length() == 0) {
           return null;
       }
       if (GlobalDatas.USECOUCHBASE) {
           JsonNode vt = cba.getFromId(id);
           if (vt != null) {
               ResultCouchbase ri = (ResultCouchbase) createOneResult();
               ri.setId(this, id);
               ri.loadFromJson(vt);
               return ri;
           }
           return null;
       } else if (GlobalDatas.USELRUCACHE) {
           // Couchbase
           return ResultLRU.LRU_ResultCached.get(id);
       } else if (GlobalDatas.USEREDIS) {
           JsonNode vt = ra.getFromId(id);
           if (vt != null) {
               ResultRedis ri = (ResultRedis) createOneResult();
               ri.setId(this, id);
               ri.loadFromJson(vt);
               return ri;
           }
           return null;
       } else {
           String nid = createDigest(id);
           ResultMongodb rm = (ResultMongodb) requests.collection.findOne(nid);
           if (rm != null) {
               rm.getAfterLoad();
               rm.loaded = true;
           }
           return rm;
       }
   }
    /**
    *
    * @param id (possibly the modified id already)
    * @return the ResultInterface if any (null else)
    */
    public final ResultInterface reload(final String id) {
       if (id == null || id.length() == 0) {
           return null;
       }
       if (GlobalDatas.USECOUCHBASE) {
           JsonNode vt = cba.getFromId(id);
           if (vt != null) {
               ResultCouchbase ri = (ResultCouchbase) createOneResult();
               ri.setId(this, id);
               ri.loadFromJson(vt);
               return ri;
           }
           return null;
       } else if (GlobalDatas.USELRUCACHE) {
           // Couchbase
           return ResultLRU.LRU_ResultCached.get(id);
       } else if (GlobalDatas.USEREDIS) {
           JsonNode vt = ra.getFromId(id);
           if (vt != null) {
               ResultRedis ri = (ResultRedis) createOneResult();
               ri.setId(this, id);
               ri.loadFromJson(vt);
               return ri;
           }
           return null;
       } else {
           ResultMongodb rm = (ResultMongodb) requests.collection.findOne(id);
           if (rm != null) {
               rm.getAfterLoad();
               rm.loaded = true;
           }
           return rm;
       }
   }

    /**
     *
     * @param collection
     *            domain of request
     * @param condition
     *            where condition
     * @param idProjection
     *            select condition
     * @return the DbCursor on the find request based on the given collection
     */
    public final DBCursor find(final VitamCollection collection, final BasicDBObject condition, final BasicDBObject idProjection) {
        return collection.collection.find(condition, idProjection);
    }

    /**
     *
     * @param indexName
     * @param type
     * @param currentNodes
     *            current parent nodes
     * @param subdepth the relative depth
     * @param condition
     * @param filterCond
     * @param useStart True if currentNodes are final ids subsets (not parents)
     * @return the ResultCached associated with this request. 
     *         Note that the exact depth is not checked, so it must be checked
     *         after (using checkAncestor method)
     */
    public final ResultInterface getSubDepth(final String indexName, final String type, final Collection<String> currentNodes,
            final int subdepth, final QueryBuilder condition, final FilterBuilder filterCond, final boolean useStart) {
        if (useStart) {
            return es.getSubDepthStart(indexName, type, currentNodes.toArray(new String[0]), subdepth, condition, filterCond);
        } else {
            return es.getSubDepth(indexName, type, currentNodes.toArray(new String[0]), subdepth, condition, filterCond);
        }
    }

   /**
    *
    * @param indexName
    * @param type
    * @param subset subset of valid nodes
    * @param condition
    * @param filterCond
    * @return the ResultCached associated with this request
    */
   public final ResultInterface getNegativeSubDepth(final String indexName, final String type, final Collection<String> subset,
           final QueryBuilder condition, final FilterBuilder filterCond) {
       return es.getNegativeSubDepth(indexName, type, subset.toArray(new String[0]), condition, filterCond);
   }

    /**
     * Add indexes to ES model
     *
     * @param indexes
     * @param model
     */
    public final void addEsEntryIndex(final Map<String, String> indexes, final String model) {
        addEsEntryIndex(GlobalDatas.BLOCKING, indexes, model);
    }

    /**
     * Add indexes to ES model
     *
     * @param blocking
     * @param indexes
     * @param model
     */
    public final void addEsEntryIndex(final boolean blocking, final Map<String, String> indexes, final String model) {
        if (blocking) {
            es.addEntryIndexesBlocking(GlobalDatas.INDEXNAME, model, indexes);
        } else {
            es.addEntryIndexes(GlobalDatas.INDEXNAME, model, indexes);
        }
    }

    /**
     * Add a Link according to relation defined, where the relation is defined in obj1->obj2 way by default (even if symmetric)
     *
     * @param obj1
     * @param relation
     * @param obj2
     * @return a {@link DBObject} that hold a possible update part (may be null)
     */
    protected final DBObject addLink(final VitamType obj1, final VitamLinks relation, final VitamType obj2) {
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
                return MongoDbAccess.addSymmetricLink(obj1, relation.field1to2, obj2, relation.field2to1);
            case SymLinkN1:
                return MongoDbAccess.addReverseSymmetricLink(obj1, relation.field1to2, obj2, relation.field2to1);
            case SymLinkNN:
                return MongoDbAccess.addSymmetricLinkset(obj1, relation.field1to2, obj2, relation.field2to1);
            case SymLink_N_N:
                return addAsymmetricLinkset(obj2, relation.field2to1, obj1, true);
            default:
                break;
        }
        return null;
    }

    /**
     * Update the link
     *
     * @param obj1
     * @param vtReloaded
     * @param relation
     * @param src
     * @return the update part
     */
    protected final BasicDBObject updateLink(final VitamType obj1, final VitamType vtReloaded, final VitamLinks relation,
            final boolean src) {
        // DBCollection coll = (src ? relation.col1.collection : relation.col2.collection);
        final String fieldname = (src ? relation.field1to2 : relation.field2to1);
        // VitamType vt = (VitamType) coll.findOne(new BasicDBObject("_id", obj1.get("_id")));
        if (vtReloaded != null) {
            String srcOid = (String) vtReloaded.remove(fieldname);
            final String targetOid = (String) obj1.get(fieldname);
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
     *
     * @param obj1
     * @param vtReloaded
     * @param relation
     * @param src
     * @return the update part
     */
    protected final BasicDBObject updateLinks(final VitamType obj1, final VitamType vtReloaded, final VitamLinks relation,
            final boolean src) {
        // DBCollection coll = (src ? relation.col1.collection : relation.col2.collection);
        final String fieldname = (src ? relation.field1to2 : relation.field2to1);
        // VitamType vt = (VitamType) coll.findOne(new BasicDBObject("_id", obj1.get("_id")));
        if (vtReloaded != null) {
            @SuppressWarnings("unchecked")
            final List<String> srcList = (List<String>) vtReloaded.remove(fieldname);
            @SuppressWarnings("unchecked")
            final List<String> targetList = (List<String>) obj1.get(fieldname);
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
            if (!obj1.containsField(fieldname)) {
                obj1.put(fieldname, new ArrayList<>());
            }
        }
        return null;
    }

    /**
     * Update links (not saved to database but to file)
     *
     * @param obj1
     * @param relation
     * @param src
     */
    protected final void updateLinksToFile(final VitamType obj1, final VitamLinks relation, final boolean src) {
        final String fieldname = (src ? relation.field1to2 : relation.field2to1);
        // nothing since save will be done just after, except checking array exists
        if (!obj1.containsField(fieldname)) {
            obj1.put(fieldname, new ArrayList<>());
        }
    }

    /**
     * Add an asymmetric relation (n-1) between Obj1 and Obj2
     *
     * @param obj1
     * @param obj1ToObj2
     * @param obj2
     * @param obj2ToObj1
     * @return a {@link DBObject} for update
     */
    private final static DBObject addReverseSymmetricLink(final VitamType obj1, final String obj1ToObj2, final VitamType obj2,
            final String obj2ToObj1) {
        addAsymmetricLinkset(obj1, obj1ToObj2, obj2, false);
        return addAsymmetricLinkUpdate(obj2, obj2ToObj1, obj1);
    }

    /**
     * Add an asymmetric relation (1-n) between Obj1 and Obj2
     *
     * @param obj1
     * @param obj1ToObj2
     * @param obj2
     * @param obj2ToObj1
     * @return a {@link DBObject} for update
     */
    private final static DBObject addSymmetricLink(final VitamType obj1, final String obj1ToObj2, final VitamType obj2,
            final String obj2ToObj1) {
        addAsymmetricLink(obj1, obj1ToObj2, obj2);
        return addAsymmetricLinkset(obj2, obj2ToObj1, obj1, true);
    }

    /**
     * Add a symmetric relation (n-n) between Obj1 and Obj2
     *
     * @param obj1
     * @param obj1ToObj2
     * @param obj2
     * @param obj2ToObj1
     * @return a {@link DBObject} for update
     */
    private final static DBObject addSymmetricLinkset(final VitamType obj1, final String obj1ToObj2, final VitamType obj2,
            final String obj2ToObj1) {
        addAsymmetricLinkset(obj1, obj1ToObj2, obj2, false);
        return addAsymmetricLinkset(obj2, obj2ToObj1, obj1, true);
    }

    /**
     * Add a single relation (1) from Obj1 to Obj2
     *
     * @param obj1
     * @param obj1ToObj2
     * @param obj2
     */
    private final static void addAsymmetricLink(final VitamType obj1, final String obj1ToObj2, final VitamType obj2) {
        final String refChild = (String) obj2.get(VitamType.ID);
        obj1.put(obj1ToObj2, refChild);
    }

    /**
     * Add a single relation (1) from Obj1 to Obj2 in update mode
     *
     * @param db
     * @param obj1
     * @param obj1ToObj2
     * @param obj2
     * @return a {@link DBObject} for update
     */
    private final static DBObject addAsymmetricLinkUpdate(final VitamType obj1, final String obj1ToObj2, final VitamType obj2) {
        final String refChild = (String) obj2.get(VitamType.ID);
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
     *
     * @param obj1
     * @param obj1ToObj2
     * @param obj2
     * @return true if the link is updated
     */
    protected final static boolean addAsymmetricLinksetNoSave(final VitamType obj1, final String obj1ToObj2,
            final VitamType obj2) {
        @SuppressWarnings("unchecked")
        ArrayList<String> relation12 = (ArrayList<String>) obj1.get(obj1ToObj2);
        final String oid2 = (String) obj2.get(VitamType.ID);
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
     *
     * @param obj1
     * @param obj1ToObj2
     * @param obj2
     * @param toUpdate
     *            True if this element will be updated through $addToSet only
     * @return a {@link DBObject} for update
     */
    private final static DBObject addAsymmetricLinkset(final VitamType obj1, final String obj1ToObj2, final VitamType obj2,
            final boolean toUpdate) {
        @SuppressWarnings("unchecked")
        ArrayList<String> relation12 = (ArrayList<String>) obj1.get(obj1ToObj2);
        final String oid2 = (String) obj2.get(VitamType.ID);
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

    /**
     * 
     * @return a new ResultInterface
     */
    public static ResultInterface createOneResult() {
        if (GlobalDatas.USECOUCHBASE) {
            return new ResultCouchbase();
        } else if (GlobalDatas.USELRUCACHE) {
            return new ResultLRU();
        } else if (GlobalDatas.USEREDIS) {
            return new ResultRedis();
        } else {
            return new ResultMongodb();
        }
    }
    /**
     * 
     * @param collection 
     * @return a new ResultInterface
     */
    public static ResultInterface createOneResult(Collection<String> collection) {
        if (GlobalDatas.USECOUCHBASE) {
            return new ResultCouchbase(collection);
        } else if (GlobalDatas.USELRUCACHE) {
            return new ResultLRU(collection);
        } else if (GlobalDatas.USEREDIS) {
            return new ResultRedis(collection);
        } else {
            return new ResultMongodb(collection);
        }
    }

}
