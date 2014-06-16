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
package fr.gouv.vitam.query.exec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.BSONObject;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import fr.gouv.vitam.mdbtypes.DAip;
import fr.gouv.vitam.mdbtypes.Domain;
import fr.gouv.vitam.mdbtypes.ElasticSearchAccess;
import fr.gouv.vitam.mdbtypes.GlobalDatas;
import fr.gouv.vitam.mdbtypes.MongoDbAccess;
import fr.gouv.vitam.mdbtypes.ResultCached;
import fr.gouv.vitam.mdbtypes.MongoDbAccess.VitamCollections;
import fr.gouv.vitam.query.exception.InvalidExecOperationException;
import fr.gouv.vitam.query.parser.AbstractQueryParser;
import fr.gouv.vitam.query.parser.ParserTokens.REQUESTFILTER;
import fr.gouv.vitam.query.parser.TypeRequest;
import fr.gouv.vitam.utils.UUID;

/**
 * @author "Frederic Bregier"
 *
 */
public class DbRequest {
	private MongoDbAccess mdAccess;
	private String indexName;
	private String typeName;
	//future private CouchbaseAccess cbAccess = new ...;
	boolean debug = true;
	boolean simulate = true;
	
	public DbRequest(MongoClient mongoClient, String dbname, String esname, String unicast, 
			boolean recreate, String indexName, String typeName) {
		mdAccess = new MongoDbAccess(mongoClient, dbname, esname, unicast, recreate);
		this.indexName = indexName;
		this.typeName = typeName;
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

	private final ResultCached createPathEntry(Collection<String> collection) {
		ResultCached path = new ResultCached();
		path.currentMaip.addAll(collection);
		path.updateMinMax();
		// Path list so as loaded (never cached)
		path.loaded = true;
		return path;
	}
	private final void computeKey(StringBuilder curId, String source) {
		curId.append('{');
		curId.append(source);
		curId.append('}');
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
		List<ResultCached> list = new ArrayList<ResultCached>(query.requests.size()+1);
		StringBuilder curId = new StringBuilder();
		// Init the list with startSet
		ResultCached result = new ResultCached();
		result.putAll((BSONObject) startSet);
		// Path list so as loaded (never cached)
		result.loaded = true;
		result.getAfterLoad();
		list.add(result);
		// cache entry search
		int lastCacheRank = searchCacheEntry(query, curId, list);
		// Get last from list and load it if not already
		result = list.get(list.size()-1);
		if (! result.loaded) {
			result.load(mdAccess);
		}
		
		String orderBy = "";
		if (query.orderBy != null) {
			orderBy = REQUESTFILTER.orderby.name()+": {"+query.orderBy.toString()+"}";
		}
		// Now from the lastlevel cached+1, execute each and every request
		// Stops if no result (empty)
		for (int rank = lastCacheRank+1; 
				(!result.currentMaip.isEmpty()) &&
				rank < query.requests.size(); rank++) {
			TypeRequest request = query.requests.get(rank);
			ResultCached newResult = executeRequest(request, result);
			if (newResult != null && ! newResult.currentMaip.isEmpty()) {
				// Compute next id
				computeKey(curId, query.sources.get(rank));
				String key = curId.toString()+orderBy;
				newResult.setId(key);
				list.add(newResult);
				result = newResult;
				if (! result.loaded) {
					result.save(mdAccess);
				}
			} else {
				result.clear();
			}
		}
		return list;
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
		// Cache concerns: request and orderBy, not limit, offset, projection
		String orderBy = "";
		if (query.orderBy != null) {
			orderBy = REQUESTFILTER.orderby.name()+": {"+query.orderBy.toString()+"}";
		}
		int lastCacheRank = -1;
		for (int rank = 0; rank < query.requests.size(); rank++) {
			TypeRequest subrequest = query.requests.get(rank);
			if (subrequest.refId != null && ! subrequest.refId.isEmpty()) {
				// ignore previous steps since results already known
				curId.setLength(0);
				computeKey(curId, query.sources.get(rank));
				ResultCached start = createPathEntry(subrequest.refId);
				start.setId(curId.toString()+orderBy);
				lastCacheRank = rank;
				list.add(start);
				continue;
			}
			// build the cache id
			computeKey(curId, query.sources.get(rank));
			// now search into the cache
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
	 * 
	 * @param previous
	 * @param next
	 * @return True if previous contains ancestors for next current
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private boolean checkAncestor(ResultCached previous, ResultCached next) throws InstantiationException, IllegalAccessException {
		Set<String> previousLastSet = new HashSet<String>();
		for (String id : previous.currentMaip) {
			previousLastSet.add(UUID.getLast(id).toString());
		}
		Set<String> nextFirstSet = new HashSet<String>();
		for (String id : next.currentMaip) {
			nextFirstSet.add(UUID.getFirst(id).toString());
		}
		for (String id : nextFirstSet) {
			DAip aip = DAip.findOne(mdAccess, id);
			HashMap<String, Integer> fathers = aip.getDomDepth();
			Set<String> fathersIds = fathers.keySet();
			fathersIds.retainAll(previousLastSet);
			if (fathers.isEmpty()) {
				// issue there
				return false;
			}
		}
		return true;
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
			ResultCached result = createPathEntry(request.refId);
			result.previousMaip = previous.currentMaip;
			// now check if path is a correct successor of previous result
			if (! checkAncestor(previous, result)) {
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
			throws InvalidExecOperationException {
		// must be MD
		if (request.isOnlyES) {
			throw new InvalidExecOperationException("Expression is not valid for Domain");
		}
		
		String srequest = request.requestModel[AbstractQueryParser.MongoDB].toString();
		BasicDBObject condition = (BasicDBObject) JSON.parse(srequest);
		BasicDBObject idProjection = new BasicDBObject("_id", 1).append(DAip.NBCHILD, 1);
		ResultCached newResult = new ResultCached();
		newResult.minLevel = 1;
		newResult.maxLevel = 1;
		if (simulate) {
			System.out.println("ReqDomain: "+condition+"\n\t"+idProjection);
			newResult.nbSubNodes = 1;
			newResult.previousMaip = previous.currentMaip;
			return newResult;
		}
		if (debug) System.out.println("ReqDomain: "+condition+"\n\t"+idProjection);
		if (GlobalDatas.printRequest) System.err.println("ReqDomain: "+condition+"\n\t"+idProjection);
		DBCursor cursor = mdAccess.domains.collection.find(condition, idProjection);
		long tempCount = 0;
		while (cursor.hasNext()) {
			Domain dom = (Domain) cursor.next();
			String mid = (String) dom.get("_id");
			newResult.currentMaip.add(mid);
			tempCount += dom.getLong(Domain.NBCHILD);
		}
		cursor.close();
		newResult.nbSubNodes = tempCount;
		// XXX FIXME filter
		newResult.previousMaip = previous.currentMaip;
		if (debug) {
			newResult.putBeforeSave();
			System.out.println("Dom: " + newResult.toString());
		}
		return newResult;
	}
	
	private final ResultCached getRequest1LevelMaipFromES(TypeRequest request, 
			ResultCached previous) throws InvalidExecOperationException {
		// must be ES
		if ((previous.nbSubNodes > GlobalDatas.limitES) || request.isOnlyES) {
			String [] aroots = previous.currentMaip.toArray(new String [1]);
			String srequest = request.requestModel[AbstractQueryParser.ElasticSearch].toString();
			String sfilter = request.filterModel[AbstractQueryParser.ElasticSearch].toString();
			QueryBuilder query = ElasticSearchAccess.getQueryFromString(srequest);
			FilterBuilder filter = (sfilter != null ? 
					ElasticSearchAccess.getFilterFromString(sfilter) : null);
			if (simulate) {
				System.out.println("Req1LevelES: "+srequest+"\n\t"+sfilter);
				ResultCached falseResult = new ResultCached();
				falseResult.minLevel = previous.minLevel+1;
				falseResult.maxLevel = previous.maxLevel+1;
				falseResult.nbSubNodes = 1;
				falseResult.previousMaip = previous.currentMaip;
				return falseResult;
			}
			if (debug) System.out.println("Req1LevelES: "+srequest+"\n\t"+sfilter);
			if (GlobalDatas.printRequest) System.err.println("Req1LevelES: "+srequest+"\n\tFilter: "+sfilter);
			ResultCached subresult = 
					mdAccess.es.getSubDepth(indexName, typeName, aroots, 1, query, filter);
			if (subresult != null && ! subresult.isEmpty()) {
				subresult.minLevel = previous.minLevel+1;
				subresult.maxLevel = previous.maxLevel+1;
				// XXX FIXME filter
				subresult.previousMaip = previous.currentMaip;
				if (debug) {
					subresult.putBeforeSave();
					System.out.println("MetaAip: "+subresult.toString());
				}
			}
			return subresult;
		} else {
			throw new InvalidExecOperationException("Expression is not valid for Maip Level 1 with ES only");
		}
	}
	
	private static final BasicDBObject idNbchildDomdepths = new BasicDBObject("_id", 1).append(DAip.NBCHILD, 1);
	//XXX FIXME .append(DAip.DAIPDEPTHS, 1);
	
	private final ResultCached getRequest1LevelMaipFromMD(TypeRequest request, 
			ResultCached previous) throws InvalidExecOperationException {
		BasicDBObject query = null;
		if (previous.minLevel == 1) {
			query = getInClauseForField(MongoDbAccess.VitamLinks.Domain2DAip.field2to1, 
					previous.currentMaip);
		} else {
			query = getInClauseForField(MongoDbAccess.VitamLinks.DAip2DAip.field2to1, 
					previous.currentMaip);
		}
		String srequest = request.requestModel[AbstractQueryParser.MongoDB].toString();
		BasicDBObject condition = (BasicDBObject) JSON.parse(srequest);
		query.putAll((BSONObject) condition);
		ResultCached subresult = new ResultCached();
		subresult.minLevel = previous.minLevel+1;
		subresult.maxLevel = previous.maxLevel+1;
		// XXX FIXME filter
		subresult.previousMaip = previous.currentMaip;
		if (simulate) {
			System.out.println("Req1LevelMD: "+query+"\n\t"+idNbchildDomdepths);
			return subresult;
		}
		if (debug) System.out.println("Req1LevelMD: "+query+"\n\t"+idNbchildDomdepths);
		if (GlobalDatas.printRequest) System.err.println("Req1LevelMD: "+query+"\n\t"+idNbchildDomdepths);
		DBCursor cursor = mdAccess.daips.collection.find(query, idNbchildDomdepths);
		long tempCount = 0;
		while (cursor.hasNext()) {
			DAip maip= (DAip) cursor.next();
			String mid = (String) maip.get("_id");
			subresult.currentMaip.add(mid);
			tempCount += maip.getLong(Domain.NBCHILD);
		}
		cursor.close();
		subresult.nbSubNodes = tempCount;
		if (debug) {
			subresult.putBeforeSave();
			System.out.println("MetaAip2: "+subresult.toString());
		}
		return subresult;
	}
	
	private final ResultCached getRequestDepth(TypeRequest request, 
		ResultCached previous) throws InvalidExecOperationException {
		// request on MAIP with depth using ES
		int subdepth = request.depth;
		String [] aroots = previous.currentMaip.toArray(new String [1]);
		String srequest = request.requestModel[AbstractQueryParser.ElasticSearch].toString();
		String sfilter = request.filterModel[AbstractQueryParser.ElasticSearch].toString();
		QueryBuilder query = ElasticSearchAccess.getQueryFromString(srequest);
		FilterBuilder filter = (sfilter != null ? ElasticSearchAccess.getFilterFromString(sfilter) : null);
		if (simulate) {
			System.out.println("ReqDepth: "+srequest+"\n\tFilter: "+sfilter);
			ResultCached subresult = new ResultCached();
			subresult.minLevel = previous.minLevel+subdepth;
			subresult.maxLevel = previous.maxLevel+subdepth;
			subresult.previousMaip = previous.currentMaip;
			return subresult;
		}
		if (debug) System.out.println("ReqDepth: "+srequest+"\n\tFilter: "+sfilter);
		if (GlobalDatas.printRequest) System.err.println("ReqDepth: "+srequest+"\n\tFilter: "+sfilter);
		ResultCached subresult = 
				mdAccess.es.getSubDepth(indexName, typeName, aroots, subdepth, query, filter);
		if (subresult != null && ! subresult.isEmpty()) {
			// XXX FIXME filter
			subresult.previousMaip = previous.currentMaip;
			if (debug) {
				subresult.putBeforeSave();
				System.out.println("MetaAipDepth: "+subresult.toString());
			}
		}
		return subresult;
	}

	private final BasicDBObject getInClauseForField(String field, Collection<String> ids) {
		if (ids.size() > 0) {
			return new BasicDBObject(field, new BasicDBObject("$in", ids));
		} else {
			return new BasicDBObject(field, ids.iterator().next());
		}
	}
}
