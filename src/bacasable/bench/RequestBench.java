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
package fr.gouv.vitam.query.old.exec.bench;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.BSONObject;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import fr.gouv.vitam.mdbtypes.DAip;
import fr.gouv.vitam.mdbtypes.Domain;
import fr.gouv.vitam.mdbtypes.ElasticSearchAccess;
import fr.gouv.vitam.mdbtypes.GlobalDatas;
import fr.gouv.vitam.mdbtypes.MongoDbAccess;
import fr.gouv.vitam.mdbtypes.VitamType;
import fr.gouv.vitam.query.exception.InvalidExecOperationException;
import fr.gouv.vitam.query.old.exec.ResultRequest;
import fr.gouv.vitam.query.parser.TypeRequest;
import fr.gouv.vitam.utils.UUID;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;


/**
 * Structure:
 * refid request, copy MetaAip in array maips[] and _id = _id+_refid
 * or
 * refid request, refmaips = [ Oids ] limited to 300 000 items
 * @author "Frederic Bregier"
 * 
 */
public class RequestBench extends VitamType {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(RequestBench.class);
    
    private static final long serialVersionUID = -2179544540441187504L;
    public static final String REFREQ = "_refreq";
    public static final String REFMAIP = "_refmaips";
    public static final String REFPATH = "_refpathes";
    
    public String refreq;
    public boolean simulate = false;
    public ResultRequest resultRequest = new ResultRequest();
    
    public RequestBench(MongoDbAccess dbvitam) {
        dbvitam.requests.collection.save(this);
        refreq = this.getString("_id");
    }

    public RequestBench() {
    }
    
    public final static void addOid(Set<String> list, DBCursor cid) {
        while (cid.hasNext()) {
            String mid = (String) cid.next().get("_id");
            list.add(mid);
        }
    }

    @SuppressWarnings("unchecked")
    public final static void addOid(Set<String> list, DBCursor cid, Set<String> listImmediateChildren, String children) {
        while (cid.hasNext()) {
            DBObject obj = cid.next();
            String mid = (String) obj.get("_id");
            list.add(mid);
            listImmediateChildren.addAll((Collection<String>) obj.get(children));
        }
    }

    @Override
    public void getAfterLoad() {
        this.resultRequest.clear();
        resultRequest.lastIds = getOid();
        resultRequest.allPaths = getPathes();
    }

    @Override
    public void putBeforeSave() {
        if (resultRequest.lastIds != null) {
            this.put(REFMAIP, resultRequest.lastIds);
        }
        if (resultRequest.allPaths != null) {
            this.put(REFPATH, resultRequest.allPaths);
        }
    }
    
    @Override
    protected boolean updated(MongoDbAccess dbvitam) {
        return false;
    }

    public void save(MongoDbAccess dbvitam) {
        putBeforeSave();
        if (updated(dbvitam)) return;
        updateOrSave(dbvitam.requests);
    }

    @SuppressWarnings("unchecked")
    public final Set<String> getOid() {
        if (resultRequest.lastIds != null) {
            return resultRequest.lastIds;
        }
        resultRequest.lastIds = new HashSet<>((List<String>) this.get(REFMAIP));
        return resultRequest.lastIds;
    }

    @SuppressWarnings("unchecked")
    public final List<Map<String, Integer>> getPathes() {
        if (resultRequest.allPaths != null) {
            return resultRequest.allPaths;
        }
        List<Map<String, Integer>> list = (List<Map<String, Integer>>) this.get(REFPATH);
        resultRequest.allPaths = new ArrayList<>();
        for (Map<String, Integer> map : list) {
            Map<String, Integer> sorted = ResultRequest.sortByValue(map);
            resultRequest.allPaths.add(sorted);
        }
        return resultRequest.allPaths;
    }

    public final void delete(MongoDbAccess dbvitam) {
        this.resultRequest.clear();
        dbvitam.requests.collection.remove(new BasicDBObject("_id", this.refreq));
    }
    
    /**
     * 
     * @param dbvitam access to MD database and ES
     * @param request one request (among several)
     * @param rank the current rank (will be incremented)
     * @param start the lower bound for the current rank (in general 0)
     * @param cpts the overall counters
     * @param model the model of data to be requested (business index)
     * @param debug debug mode if True
     * @return the number of items found with this request
     * @throws InvalidExecOperationException 
     * @throws InstantiationException
     */
    public final int executeRequest(MongoDbAccess dbvitam, TypeRequest request, 
            AtomicLong rank, BenchContext bench,
            String model, boolean debug) throws InvalidExecOperationException {
        // will not start with a Domain, so populate default sets
        if (simulate) {
            resultRequest.init();
            resultRequest.lastIds.add(new UUID().toString());
        } else if (resultRequest.isEmpty()) {
            // starts from roots
            resultRequest.init();
            resultRequest.lastIds.addAll(GlobalDatas.roots);
            for (String id : GlobalDatas.roots) {
                Map<String, Integer> newhash = new LinkedHashMap<>();
                newhash.put(id, 0);
                resultRequest.allPaths.add(newhash);
            }
        }
        if (resultRequest.minDepth < 1 && request.start == START.domain) {
            return getRequestDomain(dbvitam, request, rank, bench, debug);
        } else if (! request.isDepth) {
            // Could be ES or MD
            // request on MAIP but no depth
            resultRequest.minDepth++;
            resultRequest.maxDepth++;
            try {
                // tryES
                return getRequest1LevelMaipFromES(dbvitam, request, rank, bench, model, debug);
            } catch (InvalidExecOperationException e) {
                // try MD
                return getRequest1LevelMaipFromMD(dbvitam, request, rank, bench, model, debug);
            }
        } else {
            // depth => must be ES
            return getRequestDepth(dbvitam, request, rank, bench, model, debug);
        }
    }
    
    private final int getRequestDomain(MongoDbAccess dbvitam, TypeRequest request, 
            AtomicLong rank, BenchContext bench,
            boolean debug) throws InvalidExecOperationException {
        // must be MD
        if (request.isOnlyES) {
            throw new InvalidExecOperationException("Expression is not valid for Domain");
        }
        String srequest = ParserBench.getFinalRequestMD(request, rank, bench);
        BasicDBObject condition = (BasicDBObject) JSON.parse(srequest);
        BasicDBObject idProjection = new BasicDBObject("_id", 1).append(DAip.NBCHILD, 1);
        if (simulate) {
            LOGGER.info("ReqDomain: {}\n\t{}", condition, idProjection);
            return 1;
        }
        LOGGER.debug("ReqDomain: {}\n\t{}", condition, idProjection);
        if (GlobalDatas.printRequest) {
            LOGGER.warn("ReqDomain: {}\n\t{}", condition, idProjection);
        }
        DBCursor cursor = dbvitam.domains.collection.find(condition, idProjection);
        resultRequest.init();
        long tempCount = 0;
        while (cursor.hasNext()) {
            Domain dom = (Domain) cursor.next();
            String mid = (String) dom.get("_id");
            resultRequest.lastIds.add(mid);
            Map<String, Integer> newhash = new LinkedHashMap<>();
            newhash.put(mid, resultRequest.minDepth);
            resultRequest.allPaths.add(newhash);
            tempCount += dom.getLong(Domain.NBCHILD);
        }
        cursor.close();
        resultRequest.nextIds = tempCount;
        LOGGER.debug("Dom: {}:{}", resultRequest.nextIds, resultRequest.minDepth);
        return resultRequest.size();
    }
    
    private final void finalizeResult(Collection<String> newlist, List<Map<String, Integer>> ddss, int subdepth, boolean debug) throws InvalidExecOperationException {
        // future result paths
        List<Map<String, Integer>> newpaths = new ArrayList<>();
        // algo: V path from allPath [last(idl,depth) of path], V id in newlist, V dds from ddss for id, if (idl,dist) in dds where dist<=subdepth
        // new path' = path,(id,mindepth)
        // for each previous paths
        if (debug) {
            LOGGER.debug("Final In: newlist: "+newlist.toString()+" subdepth: "+subdepth+" ddss#: "+ddss.size());
            for (Map<String, Integer> map : ddss) {
                LOGGER.debug("\tmapDdss: "+map.toString());
            }
            LOGGER.debug("AllPath#: "+resultRequest.allPaths.size());
            for (Map<String, Integer> map : resultRequest.allPaths) {
                LOGGER.debug("\tmapprev: "+map.toString());
            }
        }
        for (Map<String, Integer> path : resultRequest.allPaths) {
            Set<String> set = path.keySet();
            String idl = set.iterator().next(); // first = last element
            // now for each id in newlist
            int i = 0;
            for(String next : newlist) {
                // get corresponding dds
                // case where ddss is empty (but should not)
                if (ddss == null || ddss.isEmpty()) {
                    throw new InvalidExecOperationException("No ddss given");
                    /*
                    Map<String, Integer> newpath = new LinkedHashMap<>();
                    newpath.put(next, resultRequest.minDepth);
                    newpath.putAll(path);
                    newpaths.add(newpath);
                    */
                } else {
                    Map<String, Integer> mapdds = ddss.get(i);
                    i++;
                    if (mapdds.containsKey(idl)) {
                        if (mapdds.get(idl) <= subdepth) {
                            // correct so create a new path
                            Map<String, Integer> newpath = new LinkedHashMap<>();
                            newpath.put(next, resultRequest.minDepth);
                            newpath.putAll(path);
                            newpaths.add(newpath);
                        } else {
                            //System.err.println("id "+idl.toString()+" not <= "+subdepth+" in "+mapdds.toString());
                        }
                    } else {
                        //System.err.println("id "+idl.toString()+" not in "+mapdds.toString());
                    }
                }
            }
            path.clear();
        }
        resultRequest.clearStruct();
        resultRequest.lastIds = new HashSet<>(newlist);
        resultRequest.allPaths = newpaths;
        if (debug) {
            LOGGER.debug("NewAllPath#: "+resultRequest.allPaths.size()+":"+resultRequest.lastIds.size());
            for (Map<String, Integer> map : newpaths) {
                LOGGER.debug("Path: "+map.toString());
            }
        }
        // clean
        newlist.clear();
        for (Map<String, Integer> map : ddss) {
            map.clear();
        }
        ddss.clear();
    }
    
    private final int getRequest1LevelMaipFromES(MongoDbAccess dbvitam, TypeRequest request, 
            AtomicLong rank, BenchContext bench,
            String model, boolean debug) throws InvalidExecOperationException {
        // must be ES
        if ((resultRequest.nextIds > GlobalDatas.limitES) || request.isOnlyES) {
            List<String> roots = ElasticSearchAccess.getFromId(resultRequest.lastIds, 1);
            String key = roots.remove(roots.size()-1);
            String [] aroots = roots.toArray(new String [1]);
            String srequest = ParserBench.getFinalRequestES(request, rank, bench);
            String sfilter = ParserBench.getFinalFilter(request, rank, bench);
            QueryBuilder query = ElasticSearchAccess.getQueryFromString(srequest);
            FilterBuilder filter = (sfilter != null ? ElasticSearchAccess.getFilterFromString(sfilter) : null);
            if (simulate) {
                LOGGER.info("Req1LevelES: {}\n\tFilter: {}", srequest, sfilter);
                return 1;
            }
            LOGGER.debug("Req1LevelES: {}\n\tFilter: {}", srequest, sfilter);
            if (GlobalDatas.printRequest) {
                LOGGER.info("Req1LevelES: {}\n\tFilter: {}", srequest, sfilter);
            }
            ResultRequest subresult = 
                    dbvitam.es.getSubDepth(GlobalDatas.indexName, model, aroots, key, 1, query, filter);
            if (subresult != null && ! subresult.isEmpty()) {
                finalizeResult(subresult.lastIds, subresult.allPaths, 1, debug);
                resultRequest.nextIds = subresult.nextIds;
                LOGGER.debug("MetaAip: {}:{}", resultRequest.nextIds, resultRequest.minDepth);
            } else {
                resultRequest.clear();
            }
        } else {
            throw new InvalidExecOperationException("Expression is not valid for Maip Level 1 with ES only");
        }
        return resultRequest.size();
    }
    
    private static final BasicDBObject idNbchildDomdepths = new BasicDBObject("_id", 1).append(DAip.NBCHILD, 1).append(DAip.DAIPDEPTHS, 1);
    
    private final int getRequest1LevelMaipFromMD(MongoDbAccess dbvitam, TypeRequest request, 
            AtomicLong rank, BenchContext bench,
            String model, boolean debug) throws InvalidExecOperationException {
        BasicDBObject query = null;
        if (resultRequest.minDepth == 1) {
            query = getInClauseForField(MongoDbAccess.VitamLinks.Domain2DAip.field2to1, resultRequest.lastIds);
        } else {
            query = getInClauseForField(MongoDbAccess.VitamLinks.DAip2DAip.field2to1, resultRequest.lastIds);
        }
        String srequest = ParserBench.getFinalRequestMD(request, rank, bench);
        BasicDBObject condition = (BasicDBObject) JSON.parse(srequest);
        query.putAll((BSONObject) condition);
        if (simulate) {
            LOGGER.info("Req1LevelMD: {}\n\t{}", query, idNbchildDomdepths);
            return 1;
        }
        LOGGER.debug("Req1LevelMD: {}\n\t{}", query, idNbchildDomdepths);
        if (GlobalDatas.printRequest) {
            LOGGER.info("Req1LevelMD: {}\n\t{}", query, idNbchildDomdepths);
        }
        DBCursor cursor = dbvitam.daips.collection.find(query, idNbchildDomdepths);
        Set<String> newlist = new HashSet<>();
        List<Map<String, Integer>> paths = new ArrayList<>();
        long tempCount = 0;
        while (cursor.hasNext()) {
            DAip maip= (DAip) cursor.next();
            String mid = (String) maip.get("_id");
            newlist.add(mid);
            tempCount += maip.getLong(Domain.NBCHILD);
            Map<String, Integer> parents = maip.getDomDepth();
            Map<String, Integer> path = new LinkedHashMap<>();
            for (String name : parents.keySet()) {
                Integer distance = parents.get(name);
                path.put(name, distance);
            }
            paths.add(path);
        }
        cursor.close();
        finalizeResult(newlist, paths, 1, debug);
        resultRequest.nextIds = tempCount;
        LOGGER.debug("MetaAip2: {}:{}", resultRequest.nextIds, resultRequest.minDepth);
        return resultRequest.size();
    }
    
    private final int getRequestDepth(MongoDbAccess dbvitam, TypeRequest request, 
        AtomicLong rank, BenchContext bench,
        String model, boolean debug) throws InvalidExecOperationException {
        // request on MAIP with depth using ES
        int subdepth = request.depth;
        resultRequest.minDepth += subdepth;
        resultRequest.maxDepth += subdepth;
        List<String> roots = ElasticSearchAccess.getFromId(resultRequest.lastIds, subdepth);
        String key = roots.remove(roots.size()-1);
        String [] aroots = roots.toArray(new String [1]);
        String srequest = ParserBench.getFinalRequestES(request, rank, bench);
        String sfilter = ParserBench.getFinalFilter(request, rank, bench);
        QueryBuilder query = ElasticSearchAccess.getQueryFromString(srequest);
        FilterBuilder filter = (sfilter != null ? ElasticSearchAccess.getFilterFromString(sfilter) : null);
        if (simulate) {
            LOGGER.info("Req1Depth: {}\n\tFilter: {}", srequest, sfilter);
            return 1;
        }
        LOGGER.debug("Req1Depth: {}\n\tFilter: {}", srequest, sfilter);
        if (GlobalDatas.printRequest) {
            LOGGER.info("Req1Depth: {}\n\tFilter: {}", srequest, sfilter);
        }
        ResultRequest subresult = 
                dbvitam.es.getSubDepth(GlobalDatas.indexName, model, aroots, key, subdepth, query, filter);
        if (subresult != null && ! subresult.isEmpty()) {
            finalizeResult(subresult.lastIds, subresult.allPaths, subdepth, debug);
            resultRequest.nextIds = subresult.nextIds;
            LOGGER.debug("MetaAipDepth: {}:{}", resultRequest.nextIds, resultRequest.minDepth);
        } else {
            resultRequest.clear();
        }
        return resultRequest.size();
    }

    private final BasicDBObject getInClauseForField(String field, Collection<String> ids) {
        if (ids.size() > 0) {
            return new BasicDBObject(field, new BasicDBObject("$in", ids));
        } else {
            return new BasicDBObject(field, ids.iterator().next());
        }
    }
    
    public static void addIndexes(MongoDbAccess dbvitam) {
        dbvitam.requests.collection.createIndex(new BasicDBObject(RequestBench.REFREQ, 1));
    }
}
