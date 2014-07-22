/**
 * This file is part of Vitam Project.
 *
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * All Vitam Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Vitam is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Vitam . If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.mdbes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.bson.BSONObject;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import fr.gouv.vitam.query.exception.InvalidExecOperationException;
import fr.gouv.vitam.query.exception.InvalidUuidOperationException;
import fr.gouv.vitam.query.parser.AbstractQueryParser;
import fr.gouv.vitam.query.parser.ParserTokens.REQUESTFILTER;
import fr.gouv.vitam.query.parser.TypeRequest;
import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.UUID;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * Version using MongoDB and ElasticSearch
 *
 * @author "Frederic Bregier"
 *
 */
public class DbRequest {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(DbRequest.class);

    private final MongoDbAccess mdAccess;
    private String indexName;
    private String typeName;
    // future private CouchbaseAccess cbAccess = new ...;
    boolean debug = true;
    boolean simulate = true;
    boolean defaultUseCache = false;
    int lastCacheRankUsed = -1;
    
    /**
     * @param mongoClient
     * @param dbname
     * @param esname
     * @param unicast
     * @param recreate
     * @param indexName
     * @param typeName
     * @throws InvalidUuidOperationException
     */
    public DbRequest(final MongoClient mongoClient, final String dbname, final String esname, final String unicast,
            final boolean recreate, final String indexName, final String typeName) throws InvalidUuidOperationException {
        mdAccess = new MongoDbAccess(mongoClient, dbname, esname, unicast, recreate);
        this.indexName = indexName;
        this.typeName = typeName;
    }

    /**
     * Constructor from an existing MongoDbAccess
     *
     * @param mdAccess
     * @param indexName
     * @param typeName
     */
    public DbRequest(final MongoDbAccess mdAccess, final String indexName, final String typeName) {
        this.mdAccess = mdAccess;
        this.indexName = indexName;
        this.typeName = typeName;
    }

    /**
     * Constructor for Simulation (no DB access)
     */
    public DbRequest() {
        debug = true;
        simulate = true;
        mdAccess = null;
    }

    /**
     * @param debug
     *            the debug to set
     */
    public void setDebug(final boolean debug) {
        this.debug = debug;
    }

    /**
     * @param simulate
     *            the simulate to set
     */
    public void setSimulate(final boolean simulate) {
        this.simulate = simulate;
    }
    
    /**
     * 
     * @param useCache
     */
    public void setUseCache(final boolean useCache) {
        this.defaultUseCache = useCache;
    }

    private static final void computeKey(final StringBuilder curId, final String source) {
        curId.append(source);
    }

    private static final String getOrderByString(final AbstractQueryParser query) {
        if (query.getOrderBy() != null) {
            return "," + REQUESTFILTER.orderby.name() + ": " + query.getOrderBy().toString();
        }
        return "";
    }

    /**
     * 
     * @return the last cache rank used
     */
    public int getLastCacheRank() {
        return lastCacheRankUsed;
    }
    /**
     * The query should be already analyzed
     *
     * @param query
     * @param startSet
     *            the set of id from which the query should start
     * @return the list of key for each entry result of the request
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvalidExecOperationException
     */
    public List<ResultInterface> execQuery(final AbstractQueryParser query, final ResultInterface startSet)
            throws InstantiationException, IllegalAccessException, InvalidExecOperationException {
        boolean useCache = false;
        useCache = defaultUseCache || query.hintCache();
        final List<ResultInterface> list = new ArrayList<ResultInterface>(query.getRequests().size() + 1);
        // Init the list with startSet
        ResultInterface result = MongoDbAccess.createOneResult();
        result.putFrom(startSet);
        list.add(result);
        // cache entry search
        final StringBuilder curId = new StringBuilder();
        curId.append("{root:");
        curId.append(result.getCurrentDaip());
        curId.append('}');
        int lastCacheRank = -1;
        if (useCache) {
            lastCacheRank = searchCacheEntry(query, curId, list);
        }
        // Get last from list and load it if not already
        result = list.get(list.size() - 1);
        if (!result.isLoaded()) {
            ResultInterface result2 = mdAccess.load(result.getId());
            if (result2 == null) {
                LOGGER.error("Cannot find searched result! = "+result.getId());
                list.clear();
                return list;
            }
            result = result2;
        }
        final String orderBy = getOrderByString(query);
        // Now from the lastlevel cached+1, execute each and every request
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("Start Request from level: "+lastCacheRank+":"+list.size()+"\n\tStartup: "+result);
        }
        lastCacheRankUsed = lastCacheRank;
        if (lastCacheRank == -1) {
            // Execute first one with StartSet
            final TypeRequest request = query.getRequests().get(0);
            result = executeRequest(request, result, true);
            lastCacheRank++;
            computeKey(curId, query.getSources().get(0));
            result.setId(curId.toString() + orderBy);
            list.add(result);
            if (useCache && !result.isLoaded()) {
                // Since not loaded means really executed and therefore to be saved
                result.save(mdAccess);
            } else {
                result.setLoaded(true);
            }
        }
        // Stops if no result (empty)
        for (int rank = lastCacheRank + 1; 
                (result != null && !result.getCurrentDaip().isEmpty()) && rank < query.getRequests().size(); 
                rank++) {
            final TypeRequest request = query.getRequests().get(rank);
            if (GlobalDatas.PRINT_REQUEST) {
                LOGGER.warn("Rank: "+rank+"\n\tPrevious: "+result+"\n\tRequest: "+request);
            }
            final ResultInterface newResult = executeRequest(request, result, false);
            if (newResult != null && !newResult.getCurrentDaip().isEmpty()) {
                // Compute next id
                computeKey(curId, query.getSources().get(rank));
                final String key = curId.toString() + orderBy;
                newResult.setId(key);
                list.add(newResult);
                result = newResult;
                if (useCache && !result.isLoaded()) {
                    // Since not loaded means really executed and therefore to be saved
                    result.save(mdAccess);
                } else {
                    result.setLoaded(true);
                }
            } else {
                LOGGER.error("No result at rank: "+rank);
                // clear also the list since no result
                list.clear();
            }
            if (debug) {
                result.putBeforeSave();
                LOGGER.debug("Request: {}\n\tResult: {}", request, result);
            }
        }
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("LastResult: "+list.size());
            for (ResultInterface resultCached : list) {
                LOGGER.warn("\tResults: "+resultCached);
            }
        }
        return list;
    }

    private static final ResultInterface createFalseResult(final ResultInterface previous, final int depth) {
        final ResultInterface start = MongoDbAccess.createOneResult();
        start.getCurrentDaip().add(new UUID().toString());
        start.setNbSubNodes(1);
        start.setLoaded(true);
        if (previous != null) {
            start.setMinLevel(previous.getMinLevel() + depth);
            start.setMaxLevel(previous.getMaxLevel() + depth);
        } else {
            start.setMinLevel(1);
            start.setMaxLevel(1);
        }
        start.putBeforeSave();
        return start;
    }
    
    private ResultInterface validFirstLevel(final AbstractQueryParser query, final StringBuilder curId, final List<ResultInterface> list)
            throws InstantiationException, IllegalAccessException {
        ResultInterface startup = list.get(0);
        Set<String> startupNodes = new HashSet<String>();
        for (String string : startup.getCurrentDaip()) {
            startupNodes.add(UUID.getLastAsString(string));
        }
        final String orderBy = getOrderByString(query);
        final TypeRequest subrequest = query.getRequests().get(0);
        if (subrequest.refId != null && !subrequest.refId.isEmpty()) {
            // Path request
            // ignore previous steps since results already known
            curId.setLength(0);
            computeKey(curId, query.getSources().get(0));
            final ResultInterface start = MongoDbAccess.createOneResult(subrequest.refId);
            start.setId(curId.toString() + orderBy);
            // Now check if current results are ok with startup
            Set<String> firstNodes = new HashSet<String>();
            for (String idsource : start.getCurrentDaip()) {
                if (simulate || UUID.isInPath(idsource, startupNodes)) {
                    firstNodes.add(idsource);
                }
            }
            start.setCurrentDaip(firstNodes);
            start.updateMinMax();
            list.add(start);
            if (debug) {
                start.putBeforeSave();
                LOGGER.debug("CacheResult: ({}) {}\n\t{}", 0, start, start.getCurrentDaip());
            }
            return start;
        }
        // build the cache id
        computeKey(curId, query.getSources().get(0));
        String newId = curId.toString() + orderBy;
        // now search into the cache
        if (simulate) {
            final ResultInterface start = createFalseResult(null, 1);
            start.setId(newId);
            list.add(start);
            if (debug) {
                LOGGER.debug("CacheResult2: ({}) {}\n\t{}", 0, start, start.getCurrentDaip());
            }
            // Only one step cached !
            return start;
        } else {
            ResultInterface loaded = mdAccess.load(newId);
            if (loaded == null) {
                // Continue looking for cache but this one ignore (sublevels can still be cached
                // while the upper one is not, due to high number of results for instance)
                if (GlobalDatas.PRINT_REQUEST) {
                    LOGGER.warn("No FirstLevel found: "+newId);
                }
                return null;
            }
            // Now check if current results are ok with startup
            Set<String> firstNodes = new HashSet<String>();
            boolean differ = false;
            for (String idsource : loaded.getCurrentDaip()) {
                if (UUID.isInPath(idsource, startupNodes)) {
                    firstNodes.add(idsource);
                } else {
                    differ = true;
                }
            }
            if (differ) {
                if (firstNodes.isEmpty()) {
                    LOGGER.error("No starting item valid: from "+loaded+ " and "+startupNodes);
                    return null;
                }
                loaded.setCurrentDaip(firstNodes);
                loaded.updateMinMax();
            }
            if (GlobalDatas.PRINT_REQUEST) {
                LOGGER.warn("FirstLvel: "+loaded);
            }
            list.add(loaded);
            return loaded;
        }
    }
    /**
     * Search for the last valid cache entry (result set in cache)
     *
     * @param query
     * @param curId
     *            StringBuilder for id
     * @param list
     *            list of primary key in ResultCache database
     * @return the last level where the cache was valid from the query
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private int searchCacheEntry(final AbstractQueryParser query, final StringBuilder curId, final List<ResultInterface> list)
            throws InstantiationException, IllegalAccessException {
        // First one should check if previously the same (sub) request was already executed (cached)
        // Cache concerns: request and orderBy, but not limit, offset, projection
        final String orderBy = getOrderByString(query);
        int lastCacheRank = -1;
        ResultInterface previous = null;
        StringBuilder newCurId = new StringBuilder(curId);
        previous = validFirstLevel(query, newCurId, list);
        if (previous != null) {
            curId.setLength(0);
            curId.append(newCurId);
            // now check if first level is compatible with startup
            if (previous.getCurrentDaip().isEmpty()) {
                return 0;
            }
            lastCacheRank = 0;
        }
        for (int rank = 1; rank < query.getRequests().size(); rank++) {
            final TypeRequest subrequest = query.getRequests().get(rank);
            if (subrequest.refId != null && !subrequest.refId.isEmpty()) {
                // Path request
                // ignore previous steps since results already known
                newCurId.setLength(0);
                computeKey(newCurId, query.getSources().get(rank));
                final ResultInterface start = MongoDbAccess.createOneResult(subrequest.refId);
                start.setId(newCurId.toString() + orderBy);
                lastCacheRank = rank;
                curId.setLength(0);
                curId.append(newCurId);
                list.add(start);
                if (debug) {
                    if (previous != null && start.getMinLevel() <= 0) {
                        start.setMinLevel(previous.getMinLevel() + 1);
                        start.setMaxLevel(previous.getMaxLevel() + 1);
                        previous = start;
                    }
                    start.putBeforeSave();
                    LOGGER.debug("CacheResult: ({}) {}\n\t{}", rank, start, start.getCurrentDaip());
                }
                previous = start;
                continue;
            }
            // build the cache id
            computeKey(newCurId, query.getSources().get(rank));
            String newId = newCurId.toString() + orderBy;
            // now search into the cache
            if (simulate) {
                lastCacheRank = rank;
                final ResultInterface start = createFalseResult(previous, 1);
                start.setId(newId);
                curId.setLength(0);
                curId.append(newCurId);
                list.add(start);
                previous = start;
                if (debug) {
                    LOGGER.debug("CacheResult2: ({}) {}\n\t{}", rank, start, start.getCurrentDaip());
                }
                // Only one step cached !
                return lastCacheRank;
            } else {
                ResultInterface loaded = mdAccess.load(newId);
                if (loaded == null) {
                    // Continue looking for cache but this one ignore (sublevels can still be cached
                    // while the upper one is not, due to high number of results for instance)
                    if (GlobalDatas.PRINT_REQUEST) {
                        LOGGER.warn("NotFound NextLevel: "+rank+":"+newId);
                    }
                    continue;
                }
                ResultInterface start = loaded;
                lastCacheRank = rank;
                curId.setLength(0);
                curId.append(newCurId);
                list.add(start);
                if (GlobalDatas.PRINT_REQUEST) {
                    LOGGER.warn("Temp NextLevel: "+rank+":"+start);
                }
                previous = start;
            }
        }
        return lastCacheRank;
    }

    /**
     * Execute one request
     *
     * @param request
     * @param previous
     *            previous Result from previous level (except in level == 0 where it is the subset of valid roots)
     * @param useStart True means that first previous set is the "startup" set, not parent
     * @return the new ResultCached from this request
     * @throws InvalidExecOperationException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    private ResultInterface executeRequest(final TypeRequest request, final ResultInterface previous, final boolean useStart)
            throws InvalidExecOperationException, InstantiationException, IllegalAccessException {
        if (request.refId != null && !request.refId.isEmpty()) {
            // path command
            final ResultInterface result = MongoDbAccess.createOneResult(request.refId);
            // now check if path is a correct successor of previous result
            if (!previous.checkAncestor(mdAccess, result)) {
                // issue since this path refers to incorrect successor
                LOGGER.error("No ancestor");
                return null;
            }
            return result;
        }
        if (request.isDepth) {
            // depth => should be ES, except if negative relative depth
            return getRequestDepth(request, previous, useStart);
        } else if (previous.getMinLevel() < 1 || (previous.getMinLevel() <= 1 && useStart)) {
            return getRequestDomain(request, previous, useStart);
        } else {
            // 1 level: Could be ES or MD
            // request on MAIP but no depth
            try {
                // tryES
                return getRequest1LevelMaipFromES(request, previous, useStart);
            } catch (final InvalidExecOperationException e) {
                // try MD
                return getRequest1LevelMaipFromMD(request, previous, useStart);
            }
        }
    }

    private final ResultInterface getRequestDomain(final TypeRequest request, final ResultInterface previous, final boolean useStart)
            throws InvalidExecOperationException, InstantiationException, IllegalAccessException {
        // must be MD
        if (request.isOnlyES) {
            throw new InvalidExecOperationException("Expression is not valid for Domain");
        }
        if (request.requestModel[AbstractQueryParser.MONGODB] == null) {
            throw new InvalidExecOperationException("Expression is not valid for Domain since no Request is available");
        }
        final String srequest = request.requestModel[AbstractQueryParser.MONGODB].toString();
        final BasicDBObject condition = (BasicDBObject) JSON.parse(srequest);
        final BasicDBObject idProjection = new BasicDBObject(VitamType.ID, 1).append(DAip.NBCHILD, 1);
        final ResultInterface newResult = MongoDbAccess.createOneResult();
        newResult.setMinLevel(1);
        newResult.setMaxLevel(1);
        if (simulate) {
            LOGGER.info("ReqDomain: {}\n\t{}", condition, idProjection);
            return createFalseResult(null, 1);
        }
        LOGGER.debug("ReqDomain: {}\n\t{}", condition, idProjection);
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("ReqDomain: {}\n\t{}", condition, idProjection);
        }
        final DBCursor cursor = mdAccess.find(mdAccess.domains, condition, idProjection);
        long tempCount = 0;
        while (cursor.hasNext()) {
            final Domain dom = (Domain) cursor.next();
            final String mid = dom.getId();
            if (useStart) {
                if (previous.getCurrentDaip().contains(mid)) {
                    newResult.getCurrentDaip().add(mid);
                    tempCount += dom.getLong(Domain.NBCHILD);
                }
            } else {
                newResult.getCurrentDaip().add(mid);
                tempCount += dom.getLong(Domain.NBCHILD);
            }
        }
        cursor.close();
        newResult.setNbSubNodes(tempCount);
        // filter on Ancestor
        if (!useStart && !previous.checkAncestor(mdAccess, newResult)) {
            LOGGER.error("No ancestor");
            return null;
        }
        // Compute of MinMax if valid since path = 1 length (root)
        newResult.updateMinMax();
        if (GlobalDatas.PRINT_REQUEST) {
            newResult.putBeforeSave();
            LOGGER.warn("Dom: {}", newResult);
        }
        return newResult;
    }

    private final ResultInterface getRequest1LevelMaipFromES(final TypeRequest request, final ResultInterface previous, final boolean useStart)
            throws InvalidExecOperationException, InstantiationException, IllegalAccessException {
        // must be ES
        if ((previous.getNbSubNodes() > GlobalDatas.limitES) || request.isOnlyES) {
            if (request.requestModel[AbstractQueryParser.ELASTICSEARCH] == null) {
                throw new InvalidExecOperationException(
                        "Expression is not valid for Daip Level 1 with ES only since no ES request is available");
            }
            final String srequest = request.requestModel[AbstractQueryParser.ELASTICSEARCH].toString();
            final String sfilter = request.filterModel[AbstractQueryParser.ELASTICSEARCH] == null ? null
                    : request.filterModel[AbstractQueryParser.ELASTICSEARCH].toString();
            final QueryBuilder query = ElasticSearchAccess.getQueryFromString(srequest);
            final FilterBuilder filter = (sfilter != null ? ElasticSearchAccess.getFilterFromString(sfilter) : null);
            if (simulate) {
                LOGGER.info("Req1LevelES: {}\n\t{}", srequest, sfilter);
                return createFalseResult(previous, 1);
            }
            LOGGER.debug("Req1LevelES: {}\n\t{}", srequest, sfilter);
            if (GlobalDatas.PRINT_REQUEST) {
                LOGGER.warn("Req1LevelES: {}\n\t{}", srequest, sfilter);
            }
            final ResultInterface subresult = mdAccess.getSubDepth(indexName, typeName, previous.getCurrentDaip(), 1, query, filter, useStart);
            if (subresult != null && !subresult.getCurrentDaip().isEmpty()) {
                if (useStart) {
                    subresult.getCurrentDaip().retainAll(previous.getCurrentDaip());
                }
                // filter on Ancestor
                if (!useStart && !previous.checkAncestor(mdAccess, subresult)) {
                    LOGGER.error("No ancestor");
                    return null;
                }
                // Not updateMinMax since result is not "valid" path but node UUID and not needed
                subresult.setMinLevel(previous.getMinLevel() + 1);
                subresult.setMaxLevel(previous.getMaxLevel() + 1);
                if (GlobalDatas.PRINT_REQUEST) {
                    subresult.putBeforeSave();
                    LOGGER.warn("MetaAip: {}", subresult);
                }
            }
            return subresult;
        } else {
            throw new InvalidExecOperationException("Expression is not valid for Maip Level 1 with ES only");
        }
    }

    private static final BasicDBObject ID_NBCHILD = new BasicDBObject(VitamType.ID, 1).append(DAip.NBCHILD, 1);

    private final ResultInterface getRequest1LevelMaipFromMD(final TypeRequest request, final ResultInterface previous, final boolean useStart)
            throws InvalidExecOperationException, InstantiationException, IllegalAccessException {
        BasicDBObject query = null;
        if (request.requestModel[AbstractQueryParser.MONGODB] == null) {
            throw new InvalidExecOperationException(
                    "Expression is not valid for Daip Level 1 with MD only since no MD request is available");
        }
        if (useStart) {
            query = getInClauseForField(DAip.ID, previous.getCurrentDaip());
        } else {
            if (previous.getMinLevel() == 1) {
                query = getInClauseForField(MongoDbAccess.VitamLinks.Domain2DAip.field2to1, previous.getCurrentDaip());
            } else {
                query = getInClauseForField(MongoDbAccess.VitamLinks.DAip2DAip.field2to1, previous.getCurrentDaip());
            }
        }
        final String srequest = request.requestModel[AbstractQueryParser.MONGODB].toString();
        final BasicDBObject condition = (BasicDBObject) JSON.parse(srequest);
        query.putAll((BSONObject) condition);
        final ResultInterface subresult = MongoDbAccess.createOneResult();
        if (simulate) {
            LOGGER.info("Req1LevelMD: {}", query);
            return createFalseResult(previous, 1);
        }
        LOGGER.debug("Req1LevelMD: {}", query);
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("Req1LevelMD: {}", query);
        }
        final DBCursor cursor = mdAccess.find(mdAccess.daips, query, ID_NBCHILD);
        long tempCount = 0;
        while (cursor.hasNext()) {
            final DAip maip = (DAip) cursor.next();
            final String mid = maip.getId();
            if (useStart) {
                if (previous.getCurrentDaip().contains(mid)) {
                    subresult.getCurrentDaip().add(mid);
                    tempCount += maip.getLong(Domain.NBCHILD);
                }
            } else {
                subresult.getCurrentDaip().add(mid);
                tempCount += maip.getLong(Domain.NBCHILD);
            }
        }
        cursor.close();
        subresult.setNbSubNodes(tempCount);
        // filter on Ancestor
        if (!useStart && !previous.checkAncestor(mdAccess, subresult)) {
            LOGGER.error("No ancestor for "+query+"\n"+previous.getCurrentDaip()+" not in "+subresult.getCurrentDaip());
            return null;
        }
        // Not updateMinMax since result is not "valid" path but node UUID and not needed
        subresult.setMinLevel(previous.getMinLevel() + 1);
        subresult.setMaxLevel(previous.getMaxLevel() + 1);
        if (GlobalDatas.PRINT_REQUEST) {
            subresult.putBeforeSave();
            LOGGER.warn("MetaAip2: {}", subresult);
        }
        return subresult;
    }

    private final ResultInterface getRequestNegativeRelativeDepthFromMD(final TypeRequest request, final ResultInterface previous, final boolean useStart) 
            throws InvalidExecOperationException, InstantiationException, IllegalAccessException {
        BasicDBObject query = null;
        if (request.requestModel[AbstractQueryParser.MONGODB] == null) {
            throw new InvalidExecOperationException(
                    "Expression is not valid for Daip Level "+request.relativedepth+" with MD only since no MD request is available");
        }
        if (useStart) {
            throw new InvalidExecOperationException("Cannot make a negative path when starting up");
        }
        if (simulate) {
            LOGGER.info("Req-xLevelMD");
            return createFalseResult(previous, 1);
        }
        int distance = -request.relativedepth;
        Set<String> subset = new HashSet<String>();
        for (String prev : previous.getCurrentDaip()) {
            DAip dprev = DAip.findOne(mdAccess, prev);
            Map<String, Integer> parents = dprev.getDomDepth();
            for (Entry<String, Integer> elt : parents.entrySet()) {
                if (elt.getValue() == distance) {
                    subset.add(elt.getKey());
                }
            }
        }
        // Use ID and not graph dependencies
        query = getInClauseForField(DAip.ID, subset);
        final String srequest = request.requestModel[AbstractQueryParser.MONGODB].toString();
        final BasicDBObject condition = (BasicDBObject) JSON.parse(srequest);
        query.putAll((BSONObject) condition);
        final ResultInterface subresult = MongoDbAccess.createOneResult();
        LOGGER.debug("Req-xLevelMD: {}", query);
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("Req-xLevelMD: {}", query);
        }
        final DBCursor cursor = mdAccess.find(mdAccess.daips, query, ID_NBCHILD);
        long tempCount = 0;
        subresult.setMinLevel(previous.getMaxLevel());
        subresult.setMaxLevel(0);
        while (cursor.hasNext()) {
            final DAip maip = (DAip) cursor.next();
            final String mid = maip.getId();
            subresult.getCurrentDaip().add(mid);
            maip.load(mdAccess);
            tempCount += maip.getLong(DAip.NBCHILD);
            // Not updateMinMax since result is not "valid" path but node UUID and not needed
            int max = maip.getMaxDepth();
            if (subresult.getMaxLevel() < max) {
                subresult.setMaxLevel(max);
            }
            if (subresult.getMinLevel() > max) {
                subresult.setMinLevel(max);
            }
        }
        cursor.close();
        subresult.setNbSubNodes(tempCount);
        if (GlobalDatas.PRINT_REQUEST) {
            subresult.putBeforeSave();
            LOGGER.warn("MetaAip2: {}", subresult);
        }
        return subresult;
    }

    private final ResultInterface getRequestNegativeRelativeDepth(final TypeRequest request, final ResultInterface previous, final boolean useStart) 
            throws InvalidExecOperationException, InstantiationException, IllegalAccessException {
        if (useStart) {
            throw new InvalidExecOperationException("Cannot make a negative path when starting up");
        }
        int distance = -request.relativedepth;
        Set<String> subset = new HashSet<String>();
        for (String prev : previous.getCurrentDaip()) {
            DAip dprev = DAip.findOne(mdAccess, prev);
            Map<String, Integer> parents = dprev.getDomDepth();
            for (Entry<String, Integer> elt : parents.entrySet()) {
                if (elt.getValue() == distance) {
                    subset.add(elt.getKey());
                }
            }
        }
        final String srequest = request.requestModel[AbstractQueryParser.ELASTICSEARCH].toString();
        final String sfilter = request.filterModel[AbstractQueryParser.ELASTICSEARCH] == null ? null
                : request.filterModel[AbstractQueryParser.ELASTICSEARCH].toString();
        final QueryBuilder query = ElasticSearchAccess.getQueryFromString(srequest);
        final FilterBuilder filter = (sfilter != null ? ElasticSearchAccess.getFilterFromString(sfilter) : null);
        if (simulate) {
            LOGGER.info("ReqDepth: {}\n\t{}", srequest, sfilter);
            return createFalseResult(previous, distance);
        }
        LOGGER.debug("ReqDepth: {}\n\t{}", srequest, sfilter);
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("ReqDepth: {}\n\t{}", srequest, sfilter);
        }
        final ResultInterface subresult = mdAccess.getNegativeSubDepth(indexName, typeName, subset, query, filter);
        if (subresult != null && !subresult.getCurrentDaip().isEmpty()) {
            subresult.updateLoadMinMax(mdAccess);
            if (GlobalDatas.PRINT_REQUEST) {
                subresult.putBeforeSave();
                LOGGER.warn("MetaAipDepth: {}", subresult);
            }
        }
        return subresult;
    }
    
    private final ResultInterface getRequestDepth(final TypeRequest request, final ResultInterface previous, final boolean useStart)
            throws InvalidExecOperationException, InstantiationException, IllegalAccessException {
        if (request.relativedepth < 0 && ! request.isOnlyES) {
            // negative depth: could be done using native Database access
            return getRequestNegativeRelativeDepthFromMD(request, previous, useStart);
        }
        // request on MAIP with depth using ES if relative depth > 0 or exact depth
        if (request.requestModel[AbstractQueryParser.ELASTICSEARCH] == null) {
            throw new InvalidExecOperationException(
                    "Expression is not valid for Daip DepthRequest with ES only since no ES request is available");
        }
        // do special request using ES with negative relative depth
        if (request.relativedepth < 0) {
            return getRequestNegativeRelativeDepth(request, previous, useStart);
        }
        int subdepth = request.relativedepth;
        if (request.exactdepth != 0) {
            subdepth = request.exactdepth - previous.getMinLevel();
        }
        final String srequest = request.requestModel[AbstractQueryParser.ELASTICSEARCH].toString();
        final String sfilter = request.filterModel[AbstractQueryParser.ELASTICSEARCH] == null ? null
                : request.filterModel[AbstractQueryParser.ELASTICSEARCH].toString();
        final QueryBuilder query = ElasticSearchAccess.getQueryFromString(srequest);
        final FilterBuilder filter = (sfilter != null ? ElasticSearchAccess.getFilterFromString(sfilter) : null);
        if (simulate) {
            LOGGER.info("ReqDepth: {}\n\t{}", srequest, sfilter);
            return createFalseResult(previous, subdepth);
        }
        LOGGER.debug("ReqDepth: {}\n\t{}", srequest, sfilter);
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("ReqDepth: {}\n\t{}", srequest, sfilter);
        }
        final ResultInterface subresult = mdAccess.getSubDepth(indexName, typeName, previous.getCurrentDaip(), subdepth, query, filter, useStart);
        if (subresult != null && !subresult.getCurrentDaip().isEmpty()) {
            // filter on Ancestor
            if (!useStart && !previous.checkAncestor(mdAccess, subresult)) {
                LOGGER.error("No ancestor");
                return null;
            }
            subresult.updateLoadMinMax(mdAccess);
            if (GlobalDatas.PRINT_REQUEST) {
                subresult.putBeforeSave();
                LOGGER.warn("MetaAipDepth: {}", subresult);
            }
        }
        return subresult;
    }

    private static final BasicDBObject getInClauseForField(final String field, final Collection<String> ids) {
        if (ids.size() > 0) {
            return new BasicDBObject(field, new BasicDBObject("$in", ids));
        } else {
            return new BasicDBObject(field, ids.iterator().next());
        }
    }
    /**
     * 
     * @param startup 
     * @param paths
     * @return the final set of results
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private Set<String> checkStartup(final ResultInterface startup, final Set<String> paths) throws InstantiationException, IllegalAccessException {
        Set<String> lastResult = new HashSet<String>();
        Set<String> shallBeStartup = new HashSet<String>();
        for (String path : startup.getCurrentDaip()) {
            shallBeStartup.add(UUID.getLastAsString(path));
        }
        for (String idsource : paths) {
            if (GlobalDatas.PRINT_REQUEST) {
                LOGGER.warn(idsource+" in "+startup.getCurrentDaip());
            }
            if (simulate || UUID.isInPath(idsource, shallBeStartup)) {
                lastResult.add(idsource);
            } else {
                try {
                    LOGGER.warn("Could Not Keeping: {} ({}) from {}", idsource, UUID.getUuids(idsource), startup.getCurrentDaip());
                } catch (InvalidUuidOperationException e) {
                    LOGGER.warn("Could Not Keeping: {} ({}) from {}", idsource, UUID.getUuidNb(idsource), startup.getCurrentDaip());
                }
                if (!checkParents(idsource, startup, lastResult)) {
                    try {
                        LOGGER.warn("Not Keeping: {} ({}) from {}", idsource, UUID.getUuids(idsource), startup.getCurrentDaip());
                    } catch (InvalidUuidOperationException e) {
                        LOGGER.warn("Not Keeping: {} ({}) from {}", idsource, UUID.getUuidNb(idsource), startup.getCurrentDaip());
                    }
                }
            }
        }
        return lastResult;
    }
    /**
     * checkParents and put in paths the final valid path for this step
     * @param node
     * @param result
     * @param paths
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private final boolean checkParents(final String node, final ResultInterface result, final Set<String> paths) 
            throws InstantiationException, IllegalAccessException {
        int size = paths.size();
        final Set<String> parents = new HashSet<String>();
        final DAip daip = DAip.findOne(mdAccess, UUID.getFirstAsString(node));
        if (daip == null) {
            Domain domain = Domain.findOne(mdAccess, UUID.getFirstAsString(node));
            if (domain != null) {
                // Domain so complete path
                for (final String p : result.getCurrentDaip()) {
                    if (UUID.getFirstAsString(node).equals(UUID.getLastAsString(p))) {
                        paths.add(node);
                    }
                }
            }
            return (paths.size() - size > 0);
        }
        final Map<String, Integer> nodeParents = daip.getDomDepth();
        for (final String p : result.getCurrentDaip()) {
            if (nodeParents.containsKey(UUID.getLastAsString(p))) {
                parents.add(p);
            }
        }
        for (final String p : parents) {
            // check if path is complete (immediate parent)
            if (daip.isImmediateParent(p)) {
                paths.add(p + node);
                continue;
            }
            // Now check and computes subpathes
            final List<String> subpathes = daip.getPathesToParent(mdAccess, p);
            for (final String subpath : subpathes) {
                paths.add(p + subpath + node);
            }
        }
        return (paths.size() - size > 0);
    }
    /**
     * Compute final Result from list of result (per step)
     * 
     * @param useCache 
     * @param results
     * @return the final result
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public ResultInterface finalizeResults(boolean useCache, final List<ResultInterface> results) throws InstantiationException, IllegalAccessException {
        // Algorithm
        // Paths = 0
        // current = last(results).current
        // if (current is "full path" pathes) => build result from current + return result (end)
        // Paths = current
        // For each result from end to start
        // if (result.current is "full path" pathes or result.maxLevel == 1) => futureStop = true
        // current = Paths
        // Paths = 0
        // For each node in current
        // Parents = 0;
        // Foreach p in result.current
        // if (first(node).AllParents intersect last(p) not 0) => Parents.add(p)
        // Foreach p in Parents => Paths.add(p # node) eventually using subpath if not immediate
        // if (futureStop) => break loop on result
        // build result from Paths + return result (end)
        if (results.isEmpty()) {
            LOGGER.error("No List of results");
            return null;
        }
        useCache |= defaultUseCache;
        final Set<String> paths = new HashSet<String>();
        final Set<String> current = new HashSet<String>();
        ResultInterface finalresult = results.get(results.size() - 1);
        ResultInterface result = finalresult;
        if (! result.isLoaded()) {
            ResultInterface result2 = mdAccess.load(result.getId());
            if (result2 == null) {
                LOGGER.error("Cannot load final result! ="+result.getId());
                return null;
            }
            result = result2;
            finalresult = result;
            if (GlobalDatas.PRINT_REQUEST) {
                LOGGER.warn("Finalize: "+result);
            }
        }
        if (result.getCurrentDaip().isEmpty()) {
            LOGGER.error("No DAip in last element: "+(results.size() - 1));
            return null;
        }
        if (UUID.isMultipleUUID(result.getCurrentDaip().iterator().next())) {
            Set<String> pathfinal = checkStartup(results.get(0), result.getCurrentDaip());
            if (pathfinal.size() < result.getCurrentDaip().size()) {
                finalresult.setCurrentDaip(pathfinal);
                finalresult.updateMinMax();
                if (simulate) {
                    finalresult.setLoaded(true);
                    finalresult.putBeforeSave();
                    LOGGER.info("FinalizeResult: {}", finalresult);
                } else if (useCache) {
                    finalresult.save(mdAccess);
                }
            } else if (useCache) {
                finalresult.updateTtl(mdAccess);
            }
            return finalresult;
        }
        paths.addAll(result.getCurrentDaip());
        int lastlevel = result.getMinLevel();
        for (int rank = results.size() - 2; rank >= 1; rank--) {
            result = results.get(rank);
            if (! result.isLoaded()) {
                ResultInterface result2 = mdAccess.load(result.getId());
                if (result2 == null) {
                    LOGGER.error("Cannot load final result! =" + result.getId());
                    return null;
                }
                result = result2;
            }
            if (useCache) {
                result.updateTtl(mdAccess);
            }
            if (result.getMinLevel() > lastlevel) {
                if (GlobalDatas.PRINT_REQUEST) {
                    LOGGER.warn("Ignore step: at rank: "+rank+" = "+result.getCurrentDaip()+
                            " since "+result.getMinLevel()+" > "+lastlevel);
                }
                continue;
            }
            lastlevel = result.getMinLevel();
            if (GlobalDatas.PRINT_REQUEST) {
                LOGGER.warn("Finalize step: from "+paths+"\n\tat rank: "+rank+" = "+result.getCurrentDaip());
            }
            if (result.getCurrentDaip().isEmpty()) {
                LOGGER.error("No DAip in rank: "+rank);
                return null;
            }
            boolean futureStop = (UUID.isMultipleUUID(result.getCurrentDaip().iterator().next()));
            futureStop |= result.getMaxLevel() == 1;
            current.addAll(paths);
            paths.clear();
            if (simulate) {
                for (final String node : current) {
                    for (final String p : result.getCurrentDaip()) {
                        paths.add(p + node);
                    }
                }
            } else {
                for (final String node : current) {
                    checkParents(node, result, paths);
                }
            }
            current.clear();
            if (futureStop) {
                // Stop recursivity since path is a full path
                break;
            }
        }
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("Finalize last step: "+paths);
        }
        if (paths.isEmpty()) {
            LOGGER.error("No Final PATH");
            return null;
        }
        // Last check is with startup nodes (should we ?)
        result = results.get(0);
        Set<String> lastResult = checkStartup(result, paths);
        paths.clear();
        if (lastResult.isEmpty()) {
            LOGGER.error("No DAip in LastResult");
            return null;
        }
        finalresult.setCurrentDaip(lastResult);
        finalresult.updateMinMax();
        if (simulate) {
            finalresult.setLoaded(true);
            finalresult.putBeforeSave();
            LOGGER.info("FinalizeResult: {}", finalresult);
        } else  if (useCache) {
            finalresult.save(mdAccess);
        }
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("FINALRESULT: "+finalresult);
        }
        return finalresult;
    }
}
