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
package fr.gouv.vitam.cbes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.QueryResult;

import fr.gouv.vitam.query.exception.InvalidUuidOperationException;
import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * Couchbase Access base class
 * 
 * @author "Frederic Bregier"
 *
 */
public class CouchbaseAccess {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(CouchbaseAccess.class);
    
    private CouchbaseCluster cc = null;
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
        protected String name;
        private int rank;
        private Bucket collection;
        @SuppressWarnings("rawtypes")
        private VitamCollections(Class clasz) {
            this.clasz = clasz;
            this.name = clasz.getSimpleName();
            this.rank = ordinal();
        }
        
        protected String getName() {
            return name;
        }

        protected Bucket getCollection() {
            return collection;
        }
    }
    
    protected static class VitamCollection{
        
        private VitamCollections coll;
        protected Bucket collection;
        
        protected VitamCollection(CouchbaseCluster db, String pwd, VitamCollections coll) {
            this.coll = coll;
            if (pwd != null) {
                this.collection = db.openBucket(coll.name, pwd).toBlockingObservable().single();
            } else {
                this.collection = db.openBucket(coll.name).toBlockingObservable().single();
            }
            this.coll.collection = this.collection;
        }
        protected Bucket getCollection() {
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
     * @param hostnames array of hostname
     * @param pwd as ""
     * @param esname
     * @param unicast
     * @param recreate
     * @throws InvalidUuidOperationException
     */
    public CouchbaseAccess(String [] hostnames, String pwd, String esname, String unicast, boolean recreate) throws InvalidUuidOperationException {
    	cc = new CouchbaseCluster(hostnames);
        // Authenticate - optional
        // boolean auth = db.authenticate("foo", "bar");
        collections = new VitamCollection[VitamCollections.values().length];
        // get a collection object to work with
        domains = collections[VitamCollections.Cdomain.rank] = new VitamCollection(cc, pwd, VitamCollections.Cdomain);
        daips = collections[VitamCollections.Cdaip.rank] = new VitamCollection(cc, pwd, VitamCollections.Cdaip);
        paips = collections[VitamCollections.Cpaip.rank] = new VitamCollection(cc, pwd, VitamCollections.Cpaip);
        saips = collections[VitamCollections.Csaip.rank] = new VitamCollection(cc, pwd, VitamCollections.Csaip);
        duarefs = collections[VitamCollections.Cdua.rank] = new VitamCollection(cc, pwd, VitamCollections.Cdua);
        requests = collections[VitamCollections.Crequests.rank] = new VitamCollection(cc, pwd, VitamCollections.Crequests);
        Iterator<QueryResult> results = executeQuery(domains.collection, "SELECT * FROM "+VitamCollections.Cdomain.name);
        while (results.hasNext()) {
            QueryResult result = results.next();
            Domain domain = new Domain();
            domain.set(result.value());
            domain.getAfterLoad();
            domain.setRoot();
        }
        // elasticsearch index
        LOGGER.info("ES on cluster name: "+esname+":"+unicast);
        es = new ElasticSearchAccess(esname, unicast, GlobalDatas.localNetworkAddress);
    }
    
    protected final static Iterator<QueryResult> executeQuery(Bucket bucket, String request) {
        return bucket.query(Query.raw(request)).toBlockingObservable().getIterator();
    }
    protected final static QueryResult executeCommand(Bucket bucket, String request) {
        return bucket.query(Query.raw(request)).toBlockingObservable().first();
    }
    
    /**
     * Drop all data and index from Couchbase and ElasticSearch
     *
     * @param model
     */
    public final void reset(final String model) {
        for (int i = 0; i < collections.length; i++) {
            // XXX FIXME cannot do it efficiently from Java SDK
            //executeCommand(collections[i].collection, "");
            //collections[i].collection.drop();
        }
        es.deleteIndex(GlobalDatas.INDEXNAME);
        es.addIndex(GlobalDatas.INDEXNAME, model);
        ensureIndex();
    }
    /**
     * Close database access
     */
    public final void close() {
    	cc.disconnect();
        es.close();
    }
    /**
     * Ensure that all Couchbase database schema are indexed
     */
    public void ensureIndex() {
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
            executeCommand(daips.collection, 
                    "DROP INDEX "+VitamCollections.Cdaip.name+"."+VitamLinks.DAip2DAip.field2to1+"_IDX");
            executeCommand(daips.collection, 
                    "DROP INDEX "+VitamCollections.Cdaip.name+"."+VitamLinks.Domain2DAip.field2to1+"_IDX");
            executeCommand(daips.collection, 
                    "DROP INDEX "+VitamCollections.Cdaip.name+"."+DAip.DAIPDEPTHS+"_IDX");
        } catch (final Exception e) {
            LOGGER.error("Error while removing indexes before import", e);
        }
    }

    /**
     * Reset MongoDB Index (import optimization?)
     */
    public void resetIndexAfterImport() {
        LOGGER.info("Rebuild indexes");
        executeCommand(daips.collection, 
                "CREATE INDEX "+VitamLinks.DAip2DAip.field2to1+"_IDX ON "+VitamCollections.Cdaip.name+"("+VitamLinks.DAip2DAip.field2to1+")");
        executeCommand(daips.collection, 
                "CREATE INDEX "+VitamLinks.Domain2DAip.field2to1+"_IDX ON "+VitamCollections.Cdaip.name+"("+VitamLinks.Domain2DAip.field2to1+")");
        executeCommand(daips.collection, 
                "CREATE INDEX "+DAip.DAIPDEPTHS+"_IDX ON "+VitamCollections.Cdaip.name+"("+DAip.DAIPDEPTHS+")");
    }
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        // XXX FIXME cannot do it
        // get a list of the collections in this database and print them out
        /*
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
        */
        return builder.toString();
    }

    /**
     *
     * @return the current number of DAip
     */
    public long getDaipSize() {
        // XXX FIXME count makes a full scan !!
        QueryResult result = executeCommand(daips.collection, 
                "SELECT COUNT(*) AS count FROM "+daips.coll.name);
        return result.value().getInt("count");
    }

    /**
     *
     * @return the current number of PAip
     */
    public long getPaipSize() {
        // XXX FIXME count makes a full scan !!
        QueryResult result = executeCommand(paips.collection, 
                "SELECT COUNT(*) AS count FROM "+paips.coll.name);
        return result.value().getInt("count");
    }
    /**
     * 
     * @param collection
     * @return an empty object using the underlying class or null in case of error
     */
    protected final VitamType createEmptyVitamTyme(VitamCollections collection) {
        try {
            return (VitamType) collection.clasz.newInstance();
        } catch (InstantiationException e) {
            LOGGER.error(e);
            return null;
        } catch (IllegalAccessException e) {
            LOGGER.error(e);
            return null;
        }
    }
    /**
     * 
     * @param collection
     * @param ref
     * @return the associated VitamType
     */
    public final VitamType loadFromObjectId(VitamCollection collection, String ref) {
        JsonDocument json = collection.collection.get(ref).toBlockingObservable().first();
        if (json.status() != ResponseStatus.SUCCESS) {
            return null;
        }
        VitamType vitamType = createEmptyVitamTyme(collection.coll);
        vitamType.set(json.content());
        return vitamType;
    }
    
    /**
     * Load a BSONObject into VitamType
     * @param obj
     * @param coll
     * @return the VitamType casted object
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public final VitamType loadFromBSONObject(JsonObject obj, VitamCollections coll) throws InstantiationException, IllegalAccessException {
        VitamType vt = (VitamType)coll.clasz.newInstance();
        vt.set(obj);
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
       Iterator<QueryResult> results = executeQuery(col.collection, "SELECT * FROM "+col.name+
               " WHERE "+field+"='"+ref+"'");
       if (results.hasNext()) {
           QueryResult result = results.next();
           VitamType vitamType = createEmptyVitamTyme(col);
           vitamType.set(result.value());
           vitamType.getAfterLoad();
           return vitamType;
       }
       return null;
   }
   
    /**
     * Find the corresponding id in col collection if it exists
     * @param col
     * @param id
     * @return the VitamType casted object
     */
    public final VitamType findOne(VitamCollections col, String id) {
        if (id == null || id.length() == 0) {
            return null;
        }
        JsonDocument json = col.collection.get(id).toBlockingObservable().first();
        if (json.status() != ResponseStatus.SUCCESS) {
            return null;
        }
        VitamType vitamType = createEmptyVitamTyme(col);
        vitamType.set(json.content());
        vitamType.getAfterLoad();
        return vitamType;
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
        JsonDocument json = col.collection.get(id).toBlockingObservable().first();
        return (json.status() == ResponseStatus.SUCCESS);
    }

    /**
     * 
     * @param collection domain of request
     * @param condition where condition
     * @param idProjection select condition
     * @return the DbCursor on the find request based on the given collection
     */
    public final Iterator<QueryResult> find(VitamCollection collection, String condition, String idProjection) {
        return executeQuery(collection.collection, 
                "SELECT "+idProjection+" FROM "+collection.coll.name+" WHERE "+condition);
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
    *
    * @param indexName
    * @param type
    * @param subset subset of valid nodes
    * @param condition
    * @param filterCond
    * @return the ResultCached associated with this request
    */
   public final ResultCached getNegativeSubDepth(final String indexName, final String type, final Collection<String> subset,
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
    * @return a {@link JsonObject} that hold a possible update part (may be null)
    */
   protected final JsonObject addLink(final VitamType obj1, final VitamLinks relation, final VitamType obj2) {
       switch (relation.type) {
           case AsymLink1:
               addAsymmetricLink(obj1, relation.field1to2, obj2);
               break;
           case SymLink11:
               addAsymmetricLink(obj1, relation.field1to2, obj2);
               return addAsymmetricLinkUpdate(obj2, relation.field2to1, obj1);
           case AsymLinkN:
               addAsymmetricLinkset(obj1, relation.field1to2, obj2, false);
               break;
           case SymLink1N:
               return addSymmetricLink(obj1, relation.field1to2, obj2, relation.field2to1);
           case SymLinkN1:
               return addReverseSymmetricLink(obj1, relation.field1to2, obj2, relation.field2to1);
           case SymLinkNN:
               return addSymmetricLinkset(obj1, relation.field1to2, obj2, relation.field2to1);
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
   protected final JsonObject updateLink(final VitamType obj1, final VitamType vtReloaded, final VitamLinks relation,
           final boolean src) {
       // DBCollection coll = (src ? relation.col1.collection : relation.col2.collection);
       final String fieldname = (src ? relation.field1to2 : relation.field2to1);
       // VitamType vt = (VitamType) coll.findOne(new BasicDBObject("_id", obj1.get("_id")));
       if (vtReloaded != null) {
           String srcOid = (String) vtReloaded.removeField(fieldname);
           final String targetOid = obj1.getString(fieldname);
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
               return JsonObject.empty().put(fieldname, srcOid);
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
   protected final JsonObject updateLinks(final VitamType obj1, final VitamType vtReloaded, final VitamLinks relation,
           final boolean src) {
       // DBCollection coll = (src ? relation.col1.collection : relation.col2.collection);
       final String fieldname = (src ? relation.field1to2 : relation.field2to1);
       // VitamType vt = (VitamType) coll.findOne(new BasicDBObject("_id", obj1.get("_id")));
       if (vtReloaded != null) {
           @SuppressWarnings("unchecked")
           final List<String> srcList = (List<String>) vtReloaded.removeField(fieldname);
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
               return JsonObject.empty().put(fieldname, 
                       JsonObject.empty().put("$each", JsonArray.empty().toList().addAll(targetList)));
               //return new BasicDBObject(fieldname, new BasicDBObject("$each", targetList));
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
    * @return a {@link JsonObject} for update
    */
   private final static JsonObject addReverseSymmetricLink(final VitamType obj1, final String obj1ToObj2, final VitamType obj2,
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
    * @return a {@link JsonObject} for update
    */
   private final static JsonObject addSymmetricLink(final VitamType obj1, final String obj1ToObj2, final VitamType obj2,
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
    * @return a {@link JsonObject} for update
    */
   private final static JsonObject addSymmetricLinkset(final VitamType obj1, final String obj1ToObj2, final VitamType obj2,
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
    * @return a {@link JsonObject} for update
    */
   private final static JsonObject addAsymmetricLinkUpdate(final VitamType obj1, final String obj1ToObj2, final VitamType obj2) {
       final String refChild = (String) obj2.get(VitamType.ID);
       if (obj1.containsField(obj1ToObj2)) {
           if (obj1.get(obj1ToObj2).equals(refChild)) {
               return null;
           }
       }
       obj1.put(obj1ToObj2, refChild);
       return JsonObject.empty().put("$set", JsonObject.empty().put(obj1ToObj2, refChild));
       //return new BasicDBObject("$set", new BasicDBObject(obj1ToObj2, refChild));
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
    * @return a {@link JsonObject} for update
    */
   private final static JsonObject addAsymmetricLinkset(final VitamType obj1, final String obj1ToObj2, final VitamType obj2,
           final boolean toUpdate) {
       @SuppressWarnings("unchecked")
       ArrayList<String> relation12 = (ArrayList<String>) obj1.get(obj1ToObj2);
       final String oid2 = (String) obj2.get(VitamType.ID);
       if (relation12 == null) {
           if (toUpdate) {
               return JsonObject.empty().put("$addToSet", JsonObject.empty().put(obj1ToObj2, oid2));
               //return new BasicDBObject("$addToSet", new BasicDBObject(obj1ToObj2, oid2));
           }
           relation12 = new ArrayList<String>();
       }
       if (relation12.contains(oid2)) {
           return null;
       }
       if (toUpdate) {
           return JsonObject.empty().put("$addToSet", JsonObject.empty().put(obj1ToObj2, oid2));
           //return new BasicDBObject("$addToSet", new BasicDBObject(obj1ToObj2, oid2));
       } else {
           relation12.add(oid2);
           obj1.put(obj1ToObj2, relation12);
           return null;
       }
   }
}
