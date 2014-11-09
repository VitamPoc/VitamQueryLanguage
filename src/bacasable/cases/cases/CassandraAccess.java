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
package fr.gouv.vitam.cases;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.GlobalDatas;
import fr.gouv.vitam.utils.FileUtil;
import fr.gouv.vitam.utils.UUID;
import fr.gouv.vitam.utils.exception.InvalidParseOperationException;
import fr.gouv.vitam.utils.exception.InvalidUuidOperationException;
import fr.gouv.vitam.utils.json.JsonHandler;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * Cassandra Access base class
 *
 * @author "Frederic Bregier"
 *
 */
public class CassandraAccess {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(CassandraAccess.class);

    private static volatile Cluster cluster = null;
    protected Session session = null;
    private String dbname = null;
    private VitamCollection[] collections = null;
    protected VitamCollection domains = null;
    protected VitamCollection daips = null;
    protected VitamCollection paips = null;
    protected VitamCollection saips = null;
    protected VitamCollection duarefs = null;
    protected VitamCollection requests = null;
    private ElasticSearchAccess es = null;
    private ListenableActionFuture<BulkResponse> bulkResponseListener = null;
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
        private VitamCollection collection;

        @SuppressWarnings("rawtypes")
        private VitamCollections(final Class clasz) {
            this.clasz = clasz;
            name = clasz.getSimpleName();
            rank = ordinal();
        }

        protected String getName() {
            return name;
        }
    }

    protected static class VitamCollection {

        private final VitamCollections coll;
        private final CassandraAccess ca;
        protected VitamCollection(final CassandraAccess ca, final VitamCollections coll, final String extra) {
            this.coll = coll;
            this.coll.collection = this;
            this.ca = ca;
            ca.session.execute("CREATE TABLE IF NOT EXISTS "+coll.name+" (id varchar PRIMARY KEY, json varchar"+extra+");");
        }
        protected void drop() {
            ca.session.execute("DROP TABLE IF EXISTS "+coll.name+";");
        }
        protected void createIndex(String field) {
            ca.session.execute("CREATE INDEX IF NOT EXISTS idx_"+field+" ON "+coll.name+" ("+field+");");
        }
        protected void dropIndex(String field) {
            ca.session.execute("DROP INDEX IF EXISTS idx_"+field+";");
        }
        protected long count() {
            ResultSet rs = ca.session.execute("SELECT COUNT(*) FROM "+coll.name);
            Row row = rs.one();
            if (row != null) {
                return row.getLong(0);
            }
            return -1;
        }
        protected ResultSet getAll() {
            return ca.session.execute("SELECT * FROM "+coll.name+";");
        }
        protected ResultSet get(String id) {
            return ca.session.execute("SELECT * FROM "+coll.name+" WHERE "+VitamType.ID+" = ?;", id);
        }
        protected boolean exists(String id) {
            ResultSet rs = ca.session.execute("SELECT "+VitamType.ID+" FROM "+coll.name+" WHERE "+VitamType.ID+" = ?;", id);
            return ! rs.isExhausted();
        }
        protected VitamType getObject(ResultSet rs) throws InstantiationException, IllegalAccessException {
            Row row = rs.one();
            if (row != null) {
                VitamType vt = (VitamType) coll.clasz.newInstance();
                vt.setFromResultSetRow(row);
                return vt;
            }
            throw new InstantiationException("Cannot find element");
        }
        protected VitamType getObject(String id) throws InstantiationException, IllegalAccessException {
            ResultSet rs = get(id);
            return getObject(rs);
        }
        protected ResultSet find(String projection, String []keys, Object []values) {
            StringBuilder builder = new StringBuilder();
            builder.append("SELECT ").append(projection);
            builder.append(" FROM ").append(coll.name);
            builder.append(" WHERE ");
            for (int i = 0; i < keys.length - 1; i++) {
                builder.append(keys[i]).append(" = ? AND ");
            }
            builder.append(keys[keys.length-1]).append(" = ?;");
            PreparedStatement writeStatement = ca.session.prepare(builder.toString());
            BoundStatement bs = writeStatement.bind();
            int i = 0;
            for (Object obj : values) {
                if (obj instanceof Integer || obj instanceof Long) {
                    bs.setLong(i, (Long) obj);
                } else {
                    bs.setString(i, (String) obj);
                }
                i++;
            }
            return ca.session.execute(bs);
        }
        protected void delete(String id) {
            ca.session.execute("DELETE FROM "+coll.name+" WHERE "+VitamType.ID+" = ?;", id);
        }
        protected void save(VitamType type, int ttl) {
            String []keys = type.getSpecificKeys();
            StringBuilder builder = new StringBuilder();
            builder.append("INSERT INTO ").append(coll.name);
            builder.append(" (").append(VitamType.ID).append(", ");
            for (String key : keys) {
                builder.append(key).append(", ");
            }
            builder.append(VitamType.JSONFIELD).append(") VALUES (?, ");
            for (@SuppressWarnings("unused") String key : keys) {
                builder.append("?, ");
            }
            if (ttl > 0) {
                builder.append("?) USING TTL ").append(ttl).append(';');
            } else {
                builder.append("?);");
            }
            PreparedStatement writeStatement = ca.session.prepare(builder.toString());
            ObjectNode copy = type.deepCopy();
            BoundStatement bs = writeStatement.bind();
            int i = 0;
            bs.setString(i, type.getId());
            i++;
            for (String elt : keys) {
                Object obj = copy.remove(elt);
                if (obj instanceof Integer || obj instanceof Long) {
                    bs.setLong(i, (Long) obj);
                } else {
                    bs.setString(i, (String) obj);
                }
                i++;
            }
            try {
                bs.setString(i, JsonHandler.writeAsString(copy));
            } catch (InvalidParseOperationException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            ca.session.execute(bs);
        }
        protected void update(VitamType type, int ttl) {
            String []keys = type.getSpecificKeys();
            StringBuilder builder = new StringBuilder();
            builder.append("UPDATE ").append(coll.name);
            if (ttl > 0) {
                builder.append(" USING TTL ").append(ttl);
            }
            builder.append(" SET ");
            for (String key : keys) {
                builder.append(key).append(" = ?, ");
            }
            builder.append(VitamType.JSONFIELD).append(" = ? WHERE ");
            builder.append(VitamType.ID).append(" = ?;");
            PreparedStatement writeStatement = ca.session.prepare(builder.toString());
            ObjectNode copy = type.deepCopy();
            BoundStatement bs = writeStatement.bind();
            int i = 0;
            for (String elt : keys) {
                Object obj = copy.remove(elt);
                if (obj instanceof Integer || obj instanceof Long) {
                    bs.setLong(i, (Long) obj);
                } else {
                    bs.setString(i, (String) obj);
                }
                i++;
            }
            try {
                bs.setString(i, JsonHandler.writeAsString(copy));
            } catch (InvalidParseOperationException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            i++;
            bs.setString(i, type.getId());
            ca.session.execute(bs);
        }
        protected void update(String id, String []keys, Object []values) {
            StringBuilder builder = new StringBuilder();
            builder.append("UPDATE ").append(coll.name).append(" SET ");
            boolean first = true;
            int i = 0;
            for (String key : keys) {
                if (first) {
                    first = false;
                } else {
                    builder.append(", ");
                }
                if (values[i] instanceof Set) {
                    builder.append(key).append(" = ").append(key).append(" + ?");
                } else if (values[i] instanceof Map) {
                    builder.append(key).append(" = ").append(key).append(" + ?");
                } else {
                    builder.append(key).append(" = ?");
                }
                i++;
            }
            builder.append(" WHERE ").append(VitamType.ID).append(" = ?;");
            PreparedStatement writeStatement = ca.session.prepare(builder.toString());
            BoundStatement bs = writeStatement.bind();
            i = 0;
            for (Object obj : values) {
                if (obj instanceof Integer || obj instanceof Long) {
                    bs.setLong(i, (Long) obj);
                } else if (obj instanceof Set) {
                    bs.setSet(i, (Set) obj);
                } else if (obj instanceof Map) {
                    bs.setMap(i, (Map) obj);
                } else {
                    bs.setString(i, (String) obj);
                }
                i++;
            }
            bs.setString(i, id);
            ca.session.execute(bs);
        }
        protected void update(String id, String key, Object value) {
            StringBuilder builder = new StringBuilder();
            builder.append("UPDATE ").append(coll.name).append(" SET ");
            if (value instanceof Set) {
                builder.append(key).append(" = ").append(key).append(" + ?");
            } else if (value instanceof Map) {
                builder.append(key).append(" = ").append(key).append(" + ?");
            } else {
                builder.append(key).append(" = ?");
            }
            builder.append(" WHERE ").append(VitamType.ID).append(" = ?;");
            PreparedStatement writeStatement = ca.session.prepare(builder.toString());
            BoundStatement bs = writeStatement.bind();
            if (value instanceof Integer || value instanceof Long) {
                bs.setLong(0, (Long) value);
            } else if (value instanceof Set) {
                bs.setSet(0, (Set) value);
            } else if (value instanceof Map) {
                bs.setMap(0, (Map) value);
            } else {
                bs.setString(0, (String) value);
            }
            bs.setString(1, id);
            ca.session.execute(bs);
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
     * @param node
     *            the node address of the server
     * @param dbname
     *            the Cassandra database name
     * @param esname
     *            the ElasticSearch name
     * @param unicast
     *            the unicast addresses for ElasticSearch
     * @param recreate
     *            shall we recreate the index
     * @throws InvalidUuidOperationException
     */
    @SuppressWarnings("unused")
    public CassandraAccess(final String node, final String dbname, final String esname, final String unicast,
            final boolean recreate) throws InvalidUuidOperationException {
        synchronized (LOGGER) {
            if (cluster == null) {
                cluster = Cluster.builder().addContactPoint(node).build();
            }
        }
        session = cluster.connect();
        this.dbname = dbname;
        session.execute("CREATE KEYSPACE IF NOT EXISTS "+this.dbname+" WITH replication "
                + "= {'class': 'SimpleStrategy', 'replication_factor':1};");
        session.execute("USE "+dbname);
        // Authenticate - optional
        // boolean auth = db.authenticate("foo", "bar");

        collections = new VitamCollection[VitamCollections.values().length];
        // get a collection object to work with
        domains = collections[VitamCollections.Cdomain.rank] = new VitamCollection(this, VitamCollections.Cdomain, Domain.createTable());
        daips = collections[VitamCollections.Cdaip.rank] = new VitamCollection(this, VitamCollections.Cdaip, DAip.createTable());
        paips = collections[VitamCollections.Cpaip.rank] = new VitamCollection(this, VitamCollections.Cpaip, PAip.createTable());
        saips = collections[VitamCollections.Csaip.rank] = new VitamCollection(this, VitamCollections.Csaip, SAip.createTable());
        duarefs = collections[VitamCollections.Cdua.rank] = new VitamCollection(this, VitamCollections.Cdua, DuaRef.createTable());
        if (GlobalDatas.USELRUCACHE || GlobalDatas.USEREDIS) {
            requests = null;
            collections[VitamCollections.Crequests.rank] = null;
        } else {
            requests = collections[VitamCollections.Crequests.rank] = new VitamCollection(this, VitamCollections.Crequests, ResultMongodb.createTable());
        }
        ResultSet rs = domains.getAll();
        Domain dom = new Domain();
        for (final Row row : rs) {
            dom.setFromResultSetRow(row);
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
     * Drop all data and index from Cassandra and ElasticSearch
     *
     * @param model
     */
    public final void reset(final String model) {
        for (int i = 0; i < collections.length; i++) {
            if (collections[i] != null) {
                collections[i].drop();
            }
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
     * Close database access (ElasticSearch, Cassandra, Redis, ...)
     */
    public final void close() {
        es.close();
        if (ra != null) {
            ra.close();
        }
        this.session.close();
    }
    /**
     * To be called once only when closing the application
     */
    public final void closeFinal() {
        if (ra != null) {
            ra.finalClose();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    /**
     * Ensure that all Cassandra database schema are indexed
     */
    public void ensureIndex() {
        for (int i = 0; i < collections.length; i++) {
            if (collections[i] != null) {
                collections[i].createIndex(VitamType.ID);
            }
        }
        Domain.addIndexes(this);
        DAip.addIndexes(this);
        PAip.addIndexes(this);
        SAip.addIndexes(this);
        DuaRef.addIndexes(this);
        if (!(GlobalDatas.USELRUCACHE || GlobalDatas.USEREDIS)) {
            ResultMongodb.addIndexes(this);
        }
    }

    /**
     * Remove temporarily the Cassandra Index (import optimization?)
     */
    public void removeIndexBeforeImport() {
        try {
            daips.dropIndex(VitamLinks.DAip2DAip.field2to1);
            daips.dropIndex(VitamLinks.Domain2DAip.field2to1);
            daips.dropIndex(DAip.DAIPDEPTHS);
        } catch (final Exception e) {
            LOGGER.error("Error while removing indexes before import", e);
        }
    }

    /**
     * Reset Cassandra Index (import optimization?)
     */
    public void resetIndexAfterImport() {
        LOGGER.info("Rebuild indexes");
        daips.createIndex(VitamLinks.DAip2DAip.field2to1);
        daips.createIndex(VitamLinks.Domain2DAip.field2to1);
        daips.createIndex(DAip.DAIPDEPTHS);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        // get a list of the collections in this database and print them out
        Metadata md = cluster.getMetadata();
        
        for (final KeyspaceMetadata s : md.getKeyspaces()) {
            builder.append(s.exportAsString());
            builder.append('\n');
        }
        return builder.toString();
    }

    /**
     *
     * @return the current number of DAip
     */
    public long getDaipSize() {
        return daips.count();
    }

    /**
     *
     * @return the current number of PAip
     */
    public long getPaipSize() {
        return paips.count();
    }
    /**
     * 
     * @return the size of the Result Cache
     */
    public long getCacheSize() {
        if (GlobalDatas.USELRUCACHE) {
            return ResultLRU.count();
        } else if (GlobalDatas.USEREDIS) {
            return ra.getCount();
        } else {
            return requests.count();
        }
    }

    /**
     *
     * @param collection
     * @param ref
     * @return a VitamType generic object from ID ref value
     */
    public final VitamType loadFromObjectId(final VitamCollection collection, final String ref) {
        try {
            return (VitamType) collection.getObject(ref);
        } catch (InstantiationException | IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Load a ObjectNode into VitamType
     *
     * @param obj
     * @param coll
     * @return the VitamType casted object
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public final VitamType loadFromObjectNode(final ObjectNode obj, final VitamCollections coll) throws InstantiationException,
    IllegalAccessException {
        final VitamType vt = (VitamType) coll.clasz.newInstance();
        vt.load(obj);
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
        ResultSet rs = session.execute("SELECT * FROM "+col.name+" WHERE "+field+" = ?;", ref);
        try {
            return (VitamType) col.collection.getObject(rs);
        } catch (InstantiationException | IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
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
        return (VitamType) col.collection.getObject(id);
    }

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
            if (GlobalDatas.USELRUCACHE) {
                return ResultLRU.exists(id);
            } else if (GlobalDatas.USEREDIS) {
                return ra.exists(id);
            } else {
                String nid = createDigest(id);
                return col.collection.exists(nid);
            }
        }
        return col.collection.exists(id);
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
       if (GlobalDatas.USELRUCACHE) {
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
           ResultMongodb rm;
            try {
                rm = (ResultMongodb) requests.getObject(nid);
            } catch (InstantiationException | IllegalAccessException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
           if (rm != null) {
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
       if (GlobalDatas.USELRUCACHE) {
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
           ResultMongodb rm;
            try {
                rm = (ResultMongodb) requests.getObject(id);
            } catch (InstantiationException | IllegalAccessException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
           if (rm != null) {
               rm.loaded = true;
           }
           return rm;
       }
    }

    /**
     *
     * @param collection
     *            domain of request
     * @param keys
     *            where condition on left side
     * @param values
     *            where condition on right side
     * @param idProjection
     *            select condition
     * @return the ResultSet on the find request based on the given collection
     */
    public final ResultSet find(final VitamCollection collection, final String []keys, final Object[]values, final String idProjection) {
        return collection.find(idProjection, keys, values);
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
    * 
    * @param model
    * @param id
    * @param json
    * @return True if inserted in ES
    */
   public final boolean addEsEntryIndex(final String model, String id, String json) {
       return es.addEntryIndex(GlobalDatas.INDEXNAME, model, id, json);
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

    private final void checkPreviousBulkEs() {
        synchronized (this) {
            if (bulkResponseListener != null) {
                BulkResponse response = bulkResponseListener.actionGet();
                if (response.hasFailures()) {
                    LOGGER.error("ES previous insert in error: "+response.buildFailureMessage());
                }
                bulkResponseListener = null;
            }
        }
    }
    /**
     * Add indexes to ES model
     *
     * @param blocking
     * @param indexes
     * @param model
     * @return True if done (and if blocking)
     */
    public final boolean addEsEntryIndex(final boolean blocking, final Map<String, String> indexes, final String model) {
        checkPreviousBulkEs();
        if (blocking) {
            return es.addEntryIndexesBlocking(GlobalDatas.INDEXNAME, model, indexes);
        } else {
            synchronized(this) {
                bulkResponseListener = es.addEntryIndexes(GlobalDatas.INDEXNAME, model, indexes);
            }
            return true;
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
    protected final String addLink(final VitamType obj1, final VitamLinks relation, final VitamType obj2) {
        switch (relation.type) {
            case AsymLink1:
                switch (relation) {
                    case DAip2Dua:
                        VitamType.addDuaAsymmetricLinkset(obj1, obj2, false);
                        break;
                    default:
                        break;
                }
                break;
            case SymLink11:
                switch (relation) {
                    case PAip2SAip:
                        VitamType.addSaipAsymmetricLink(obj1, obj2);
                        return VitamType.addPaipAsymmetricLinkUpdate(obj2, obj1);
                    default:
                        break;
                }
                break;
            case AsymLinkN:
                switch (relation) {
                    case PAip2Dua:
                        VitamType.addDuaAsymmetricLinkset(obj1, obj2, false);
                        break;
                    default:
                        break;
                }
                break;
            case SymLink1N:
                switch (relation) {
                    case DAip2PAip:
                        return VitamType.addUpPaipSymmetricLink(obj1, obj2);
                    default:
                        break;
                }
                break;
            case SymLinkN1:
                switch (relation) {
                    //return VitamType.addReverseSymmetricLink(obj1, relation.field1to2, obj2, relation.field2to1);
                    default:
                        break;
                }
                break;
            case SymLinkNN:
                switch (relation) {
                    case Domain2DAip:
                        return VitamType.addDaipDomSymmetricLinkset(obj1, obj2);
                    default:
                        break;
                }
                break;
            case SymLink_N_N:
                switch (relation) {
                    case DAip2DAip:
                        return VitamType.addDaipsAsymmetricLinkset(obj2, obj1, true);
                    default:
                        break;
                }
                break;
            default:
                break;
        }
        return null;
    }


    /**
     * 
     * @return a new ResultInterface
     */
    public static ResultInterface createOneResult() {
        if (GlobalDatas.USELRUCACHE) {
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
        if (GlobalDatas.USELRUCACHE) {
            return new ResultLRU(collection);
        } else if (GlobalDatas.USEREDIS) {
            return new ResultRedis(collection);
        } else {
            return new ResultMongodb(collection);
        }
    }

}
