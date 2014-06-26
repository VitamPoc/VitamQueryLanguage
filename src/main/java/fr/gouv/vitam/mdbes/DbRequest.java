/**
   This file is part of Vitam Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All Vitam Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   Vitam is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Vitam .  If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.mdbes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BSONObject;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import fr.gouv.vitam.mdbes.MongoDbAccess.VitamCollections;
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
    
    private MongoDbAccess mdAccess;
    private String indexName;
    private String typeName;
    //future private CouchbaseAccess cbAccess = new ...;
    boolean debug = true;
    boolean simulate = true;
    
    public DbRequest(MongoClient mongoClient, String dbname, String esname, String unicast, 
            boolean recreate, String indexName, String typeName) throws InvalidUuidOperationException {
        mdAccess = new MongoDbAccess(mongoClient, dbname, esname, unicast, recreate);
        this.indexName = indexName;
        this.typeName = typeName;
    }
    public DbRequest(MongoDbAccess mdAccess, String indexName, String typeName) {
        this.mdAccess = mdAccess;
        this.indexName = indexName;
        this.typeName = typeName;
    }
    /**
     * Constructor for Simulation (no DB access)
     */
    public DbRequest() {
        this.debug = true;
        this.simulate = true;
        this.mdAccess = null;
    }
    /**
     * @param debug the debug to set
     */
    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    /**
     * @param simulate the simulate to set
     */
    public void setSimulate(boolean simulate) {
        this.simulate = simulate;
    }

    private static final void computeKey(StringBuilder curId, String source) {
        curId.append('{');
        curId.append(source);
        curId.append('}');
    }
    private static final String getOrderByString(AbstractQueryParser query) {
        if (query.getOrderBy() != null) {
            return REQUESTFILTER.orderby.name()+": {"+query.getOrderBy().toString()+"}";
        }
        return "";
    }
    /**
     * The query should be already analyzed
     * @param query
     * @param startSet the set of id from which the query should start
     * @return the list of key for each entry result of the request
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     * @throws InvalidExecOperationException 
     */
    public List<ResultCached> execQuery(AbstractQueryParser query, ResultCached startSet) 
            throws InstantiationException, IllegalAccessException, InvalidExecOperationException {
        List<ResultCached> list = new ArrayList<ResultCached>(query.getRequests().size()+1);
        StringBuilder curId = new StringBuilder();
        // Init the list with startSet
        ResultCached result = new ResultCached();
        result.putAll((BSONObject) startSet);
        // Path list so as loaded (never cached)
        result.loaded = true;
        result.getAfterLoad();
        list.add(result);
        LOGGER.debug("StartResult: {}", result);
        // cache entry search
        int lastCacheRank = searchCacheEntry(query, curId, list);
        // Get last from list and load it if not already
        result = list.get(list.size()-1);
        if (! result.loaded) {
            result.load(mdAccess);
        }
        
        String orderBy = getOrderByString(query);
        // Now from the lastlevel cached+1, execute each and every request
        // Stops if no result (empty)
        for (int rank = lastCacheRank+1; 
                (!result.currentMaip.isEmpty()) &&
                rank < query.getRequests().size(); rank++) {
            TypeRequest request = query.getRequests().get(rank);
            ResultCached newResult = executeRequest(request, result);
            if (newResult != null && ! newResult.currentMaip.isEmpty()) {
                // Compute next id
                computeKey(curId, query.getSources().get(rank));
                String key = curId.toString()+orderBy;
                newResult.setId(key);
                list.add(newResult);
                result = newResult;
                if (! result.loaded) {
                    // Since not loaded means really executed and therefore to be saved
                    result.save(mdAccess);
                }
            } else {
                // Error
                result.clear();
                // clear also the list since no result
                list.clear();
            }
            if (debug) {
                result.putBeforeSave();
                LOGGER.debug("Request: {}\n\tResult: {}", request, result);
            }
        }
        return list;
    }
    
    private static final ResultCached createFalseResult(ResultCached previous, int depth) {
        ResultCached start = new ResultCached();
        start.currentMaip.add(new UUID().toString());
        start.nbSubNodes = 1;
        start.loaded = true;
        if (previous != null) {
            start.minLevel = previous.minLevel+depth;
            start.maxLevel = previous.maxLevel+depth;
        } else {
            start.minLevel = 1;
            start.maxLevel = 1;
        }
        start.putBeforeSave();
        return start;
    }
    /**
     * Search for the last valid cache entry (result set in cache)
     * @param query
     * @param curId StringBuilder for id
     * @param list list of primary key in ResultCache database
     * @return the last level where the cache was valid from the query
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private int searchCacheEntry(AbstractQueryParser query, StringBuilder curId, List<ResultCached> list) 
            throws InstantiationException, IllegalAccessException {
        // First one should check if previously the same (sub) request was already executed (cached)
        // Cache concerns: request and orderBy, but not limit, offset, projection
        String orderBy = getOrderByString(query);
        int lastCacheRank = -1;
        ResultCached previous = null;
        for (int rank = 0; rank < query.getRequests().size(); rank++) {
            TypeRequest subrequest = query.getRequests().get(rank);
            if (subrequest.refId != null && ! subrequest.refId.isEmpty()) {
                // Path request
                // ignore previous steps since results already known
                curId.setLength(0);
                computeKey(curId, query.getSources().get(rank));
                ResultCached start = new ResultCached(subrequest.refId);
                start.setId(curId.toString()+orderBy);
                lastCacheRank = rank;
                list.add(start);
                if (debug) {
                    if (previous != null && start.minLevel <= 0) {
                        start.minLevel = previous.minLevel+1;
                        start.maxLevel = previous.maxLevel+1;
                        previous = start;
                    }
                    start.putBeforeSave();
                    LOGGER.debug("CacheResult: ({}) {}\n\t{}", rank, start, start.currentMaip);
                }
                previous = start;
                continue;
            }
            // build the cache id
            computeKey(curId, query.getSources().get(rank));
            // now search into the cache
            if (simulate) {
                lastCacheRank = rank;
                ResultCached start = createFalseResult(previous, 1);
                start.setId(curId.toString()+orderBy);
                list.add(start);
                previous = start;
                if (debug) {
                    LOGGER.debug("CacheResult2: ({}) {}\n\t{}", rank, start, start.currentMaip);
                }
                // Only one step cached !
                return lastCacheRank;
            } else
            if (mdAccess.exists(VitamCollections.Crequests, curId.toString()+orderBy)) {
                // Optimization: not loading from cache since next level could exist
                lastCacheRank = rank;
                ResultCached start = new ResultCached();
                start.setId(curId.toString()+orderBy);
                start.loaded = false;
                list.add(start);
            } else {
                // Stop looking for cache since first uncached level reached
                break;
            }
        }
        return lastCacheRank;
    }

    /**
     * Execute one request
     * @param request
     * @param previous previous Result from previous level (except in level == 0 where it is the subset of valid roots)
     * @return the new ResultCached from this request
     * @throws InvalidExecOperationException 
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     */
    private ResultCached executeRequest(TypeRequest request, ResultCached previous) 
            throws InvalidExecOperationException, InstantiationException, IllegalAccessException {
        if (request.refId != null && ! request.refId.isEmpty()) {
            // path command
            ResultCached result = new ResultCached(request.refId);
            // now check if path is a correct successor of previous result
            if (! previous.checkAncestor(mdAccess, result)) {
                // issue since this path refers to incorrect successor
                return null;
            }
            return result;
        }
        if (previous.minLevel < 1) {
            return getRequestDomain(request, previous);
        } else if (! request.isDepth) {
            // Could be ES or MD
            // request on MAIP but no depth
            try {
                // tryES
                return getRequest1LevelMaipFromES(request, previous);
            } catch (InvalidExecOperationException e) {
                // try MD
                return getRequest1LevelMaipFromMD(request, previous);
            }
        } else {
            // depth => must be ES
            return getRequestDepth(request, previous);
        }
    }
    
    private final ResultCached getRequestDomain(TypeRequest request, ResultCached previous) 
            throws InvalidExecOperationException, InstantiationException, IllegalAccessException {
        // must be MD
        if (request.isOnlyES) {
            throw new InvalidExecOperationException("Expression is not valid for Domain");
        }
        if (request.requestModel[AbstractQueryParser.MONGODB] == null) {
            throw new InvalidExecOperationException("Expression is not valid for Domain since no Request is available");
        }
        String srequest = request.requestModel[AbstractQueryParser.MONGODB].toString();
        BasicDBObject condition = (BasicDBObject) JSON.parse(srequest);
        BasicDBObject idProjection = new BasicDBObject(VitamType.ID, 1).append(DAip.NBCHILD, 1);
        ResultCached newResult = new ResultCached();
        newResult.minLevel = 1;
        newResult.maxLevel = 1;
        if (simulate) {
            LOGGER.info("ReqDomain: {}\n\t{}", condition, idProjection);
            return createFalseResult(null, 1);
        }
        LOGGER.debug("ReqDomain: {}\n\t{}", condition, idProjection);
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("ReqDomain: {}\n\t{}", condition, idProjection);
        }
        DBCursor cursor = mdAccess.find(mdAccess.domains, condition, idProjection);
        long tempCount = 0;
        while (cursor.hasNext()) {
            Domain dom = (Domain) cursor.next();
            String mid = dom.getId();
            newResult.currentMaip.add(mid);
            tempCount += dom.getLong(Domain.NBCHILD);
        }
        cursor.close();
        newResult.nbSubNodes = tempCount;
        // filter on Ancestor
        if (! previous.checkAncestor(mdAccess, newResult)) {
            return null;
        }
        // Compute of MinMax if valid since path = 1 length (root)
        newResult.updateMinMax();
        if (debug) {
            newResult.putBeforeSave();
            LOGGER.debug("Dom: " + newResult.toString());
        }
        return newResult;
    }
    
    private final ResultCached getRequest1LevelMaipFromES(TypeRequest request, 
            ResultCached previous) throws InvalidExecOperationException, InstantiationException, IllegalAccessException {
        // must be ES
        if ((previous.nbSubNodes > GlobalDatas.limitES) || request.isOnlyES) {
            if (request.requestModel[AbstractQueryParser.ELASTICSEARCH] == null) {
                throw new InvalidExecOperationException("Expression is not valid for Maip Level 1 with ES only since no ES request is available");
            }
            String srequest = request.requestModel[AbstractQueryParser.ELASTICSEARCH].toString();
            String sfilter = request.filterModel[AbstractQueryParser.ELASTICSEARCH] == null ? null :
                request.filterModel[AbstractQueryParser.ELASTICSEARCH].toString();
            QueryBuilder query = ElasticSearchAccess.getQueryFromString(srequest);
            FilterBuilder filter = (sfilter != null ? 
                    ElasticSearchAccess.getFilterFromString(sfilter) : null);
            if (simulate) {
                LOGGER.info("Req1LevelES: {}\n\t{}", srequest, sfilter);
                return createFalseResult(previous, 1);
            }
            LOGGER.debug("Req1LevelES: {}\n\t{}", srequest, sfilter);
            if (GlobalDatas.PRINT_REQUEST) {
                LOGGER.warn("Req1LevelES: {}\n\t{}", srequest, sfilter);
            }
            ResultCached subresult = 
                    mdAccess.getSubDepth(indexName, typeName, previous.currentMaip, 1, query, filter);
            if (subresult != null && ! subresult.isEmpty()) {
                // filter on Ancestor
                if (! previous.checkAncestor(mdAccess, subresult)) {
                    return null;
                }
                // Not updateMinMax since result is not "valid" path but node UUID and not needed
                subresult.minLevel = previous.minLevel+1;
                subresult.maxLevel = previous.maxLevel+1;
                if (debug) {
                    subresult.putBeforeSave();
                    LOGGER.debug("MetaAip: {}", subresult);
                }
            }
            return subresult;
        } else {
            throw new InvalidExecOperationException("Expression is not valid for Maip Level 1 with ES only");
        }
    }
    
    private static final BasicDBObject ID_NBCHILD = new BasicDBObject(VitamType.ID, 1).append(DAip.NBCHILD, 1);
    //XXX FIXME .append(DAip.DAIPDEPTHS, 1);
    
    private final ResultCached getRequest1LevelMaipFromMD(TypeRequest request, 
            ResultCached previous) throws InvalidExecOperationException, InstantiationException, IllegalAccessException {
        BasicDBObject query = null;
        if (request.requestModel[AbstractQueryParser.MONGODB] == null) {
            throw new InvalidExecOperationException("Expression is not valid for Maip Level 1 with MD only since no MD request is available");
        }
        if (previous.minLevel == 1) {
            query = getInClauseForField(MongoDbAccess.VitamLinks.Domain2DAip.field2to1, 
                    previous.currentMaip);
        } else {
            query = getInClauseForField(MongoDbAccess.VitamLinks.DAip2DAip.field2to1, 
                    previous.currentMaip);
        }
        String srequest = request.requestModel[AbstractQueryParser.MONGODB].toString();
        BasicDBObject condition = (BasicDBObject) JSON.parse(srequest);
        query.putAll((BSONObject) condition);
        ResultCached subresult = new ResultCached();
        if (simulate) {
            LOGGER.info("Req1LevelMD: {}\n\t{}", query, ID_NBCHILD);
            return createFalseResult(previous, 1);
        }
        LOGGER.debug("Req1LevelMD: {}\n\t{}", query, ID_NBCHILD);
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("Req1LevelMD: {}\n\t{}", query, ID_NBCHILD);
        }
        DBCursor cursor = mdAccess.find(mdAccess.daips, query, ID_NBCHILD);
        long tempCount = 0;
        while (cursor.hasNext()) {
            DAip maip= (DAip) cursor.next();
            String mid = maip.getId();
            subresult.currentMaip.add(mid);
            tempCount += maip.getLong(Domain.NBCHILD);
        }
        cursor.close();
        subresult.nbSubNodes = tempCount;
        // filter on Ancestor
        if (! previous.checkAncestor(mdAccess, subresult)) {
            return null;
        }
        // Not updateMinMax since result is not "valid" path but node UUID and not needed
        subresult.minLevel = previous.minLevel+1;
        subresult.maxLevel = previous.maxLevel+1;
        if (debug) {
            subresult.putBeforeSave();
            LOGGER.debug("MetaAip2: {}", subresult);
        }
        return subresult;
    }
    
    private final ResultCached getRequestDepth(TypeRequest request, 
        ResultCached previous) throws InvalidExecOperationException, InstantiationException, IllegalAccessException {
        // request on MAIP with depth using ES
        if (request.requestModel[AbstractQueryParser.ELASTICSEARCH] == null) {
            throw new InvalidExecOperationException("Expression is not valid for Maip DepthRequest with ES only since no ES request is available");
        }
        int subdepth = request.depth;
        if (request.exactdepth != 0) {
            subdepth = request.exactdepth - previous.minLevel;
        }        
        String srequest = request.requestModel[AbstractQueryParser.ELASTICSEARCH].toString();
        String sfilter = request.filterModel[AbstractQueryParser.ELASTICSEARCH] == null ? null :
            request.filterModel[AbstractQueryParser.ELASTICSEARCH].toString();
        QueryBuilder query = ElasticSearchAccess.getQueryFromString(srequest);
        FilterBuilder filter = (sfilter != null ? ElasticSearchAccess.getFilterFromString(sfilter) : null);
        if (simulate) {
            LOGGER.info("ReqDepth: {}\n\t{}", srequest, sfilter);
            return createFalseResult(previous, subdepth);
        }
        LOGGER.debug("ReqDepth: {}\n\t{}", srequest, sfilter);
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("ReqDepth: {}\n\t{}", srequest, sfilter);
        }
        ResultCached subresult = 
                mdAccess.getSubDepth(indexName, typeName, previous.currentMaip, subdepth, query, filter);
        if (subresult != null && ! subresult.isEmpty()) {
            // filter on Ancestor
            if (! previous.checkAncestor(mdAccess, subresult)) {
                return null;
            }
            subresult.updateLoadMinMax(mdAccess);
            if (debug) {
                subresult.putBeforeSave();
                LOGGER.debug("MetaAipDepth: {}", subresult);
            }
        }
        return subresult;
    }

    private static final BasicDBObject getInClauseForField(String field, Collection<String> ids) {
        if (ids.size() > 0) {
            return new BasicDBObject(field, new BasicDBObject("$in", ids));
        } else {
            return new BasicDBObject(field, ids.iterator().next());
        }
    }
    
    /**
     * Compute final Result from list of result (per step)
     * @param results
     * @return the final result
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     */
    public ResultCached finalizeResults(List<ResultCached> results) throws InstantiationException, IllegalAccessException {
        // Algorithm
        // Paths = 0
        // current = last(results).current
        // if (current is "full path" pathes) => build result from current + return result (end)
        // Paths = current
        // For each result from end to start
        //    if (result.current is "full path" pathes or result.maxLevel == 1) => futureStop = true
        //    current = Paths
        //    Paths = 0
        //    For each node in current
        //       Parents = 0;
        //       Foreach p in result.current
        //         if (first(node).AllParents intersect last(p) not 0) => Parents.add(p)
        //       Foreach p in Parents => Paths.add(p # node) eventually using subpath if not immediate
        //    if (futureStop) => break loop on result
        //  build result from Paths + return result (end)
        if (results.isEmpty()) {
            return null;
        }
        Set<String> paths = new HashSet<String>();
        Set<String> current = new HashSet<String>();
        Set<String> parents = new HashSet<String>();
        ResultCached result = results.get(results.size()-1);
        if (result.currentMaip.isEmpty()) {
            return null;
        }
        if (UUID.isMultipleUUID(result.currentMaip.iterator().next())) {
            return result;
        }
        String originalKey = result.getId();
        long subnodes = result.nbSubNodes;
        paths.addAll(result.currentMaip);
        for (int rank = results.size()-2; rank >= 0; rank --) {
            result = results.get(rank);
            if (result.currentMaip.isEmpty()) {
                return null;
            }
            boolean futureStop = (UUID.isMultipleUUID(result.currentMaip.iterator().next()));
            futureStop |= result.maxLevel == 1;
            current.addAll(paths);
            paths.clear();
            if (simulate) {
                for (String node : current) {
                    for (String p : result.currentMaip) {
                        paths.add(p+node);
                    }
                }
            } else {
                for (String node : current) {
                    parents.clear();
                    DAip daip = DAip.findOne(mdAccess, UUID.getFirstAsString(node));
                    if (daip == null) {
                        continue;
                    }
                    Map<String, Integer> nodeParents = daip.getDomDepth();
                    for (String p : result.currentMaip) {
                        if (nodeParents.containsKey(UUID.getLastAsString(p))) {
                            parents.add(p);
                        }
                    }
                    for (String p : parents) {
                        // check if path is complete (immediate parent)
                        if (daip.isImmediateParent(p)) {
                            paths.add(p+node);
                            continue;
                        }
                        // Now check and computes subpathes
                        List<String> subpathes = daip.getPathesToParent(mdAccess, p);
                        for (String subpath : subpathes) {
                            paths.add(p+subpath+node);
                        }
                    }
                }
            }
            parents.clear();
            current.clear();
            if (futureStop) {
                // Stop recursivity since path is a full path
                break;
            }
        }
        if (paths.isEmpty()) {
            return null;
        }
        result = new ResultCached();
        result.currentMaip = paths;
        result.setId(originalKey);
        result.nbSubNodes = subnodes;
        result.updateMinMax();
        if (simulate) {
            result.loaded = true;
            result.putBeforeSave();
            LOGGER.info("FinalizeResult: {}", result);
        } else {
            result.save(mdAccess);
        }
        return result;
    }
}
