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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.BSONObject;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsFilterBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import com.mongodb.BasicDBObject;

import fr.gouv.vitam.mdbes.MongoDbAccess.VitamLinks;
import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * ElasticSearch model with MongoDB main database
 * @author "Frederic Bregier"
 *
 */
public class ElasticSearchAccess {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(ElasticSearchAccess.class);
    
    private static void registerShutdownHook(final Node node ) {
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run() {
                node.close();
            }
        });
    }
    
    static Node node;
    Node localNode;
    Client client;

    /**
     * Create an ElasticSearch access
     * @param clusterName the name of the Cluster
     * @param unicast the unicast list of addresses
     * @param networkAddress the local network address (if any)
     */
    public ElasticSearchAccess(String clusterName, String unicast, String networkAddress) {
    	Settings settings = null;
    	if (networkAddress != null) {
            settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName)
                    .put("discovery.zen.ping.multicast.enabled", false)
                    .put("discovery.zen.ping.unicast.hosts", unicast)
                    .put("network.host", networkAddress)
                    .build();
    	} else {
            settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName)
                    .put("discovery.zen.ping.multicast.enabled", false)
                    .put("discovery.zen.ping.unicast.hosts", unicast)
                    .build();
    	}
        if (GlobalDatas.useNewNode) {
            localNode = NodeBuilder.nodeBuilder().clusterName(clusterName).client(true).settings(settings).node();
            registerShutdownHook(localNode);
        } else if (node == null) {
            node = NodeBuilder.nodeBuilder().clusterName(clusterName).client(true).settings(settings).node();
            registerShutdownHook(node);
            localNode = node;
        } else {
            localNode = node;
        }
        client = localNode.client();
    }
    /**
     * Close the ElasticSearch connection
     */
    public void close() {
        client.close();
        if (GlobalDatas.useNewNode) {
            localNode.close();
        }
    }
    /**
     * Delete the index
     * @param idxName
     * @return True if ok
     */
    public final boolean deleteIndex(String idxName) {
        try {
            if (client.admin().indices().prepareExists(idxName).execute().actionGet().isExists()) {
                if (!client.admin().indices().prepareDelete(idxName).execute().actionGet().isAcknowledged()) {
                    LOGGER.error("Error on index delete");
                }
            }
            return true;
        } catch (Exception e) {
            LOGGER.error("Error while deleting index", e);
            return true;
        }
    }
    /**
     * Add a type to an index
     * @param indexName
     * @param type
     * @return True if ok
     */
    public final boolean addIndex(String indexName, String type) {
		LOGGER.debug("addIndex");
        if (! client.admin().indices().prepareExists(indexName).execute().actionGet().isExists()) {
			LOGGER.debug("createIndex");
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
        }
        if (type == null) {
        	return false;
        }
        String mapping = "{"+type+
                // Will keep DAIPDEPTHS and NBCHILD as value to get (_id is implicit)
//Change since DAIPDEPTHS not useful as source                        " : { _source : { includes : [\""+DAip.DAIPDEPTHS+".*\", \""+DAip.NBCHILD+"\"] },"+
                " : { _source : { includes : [\""+DAip.NBCHILD+"\"] },"+
                // DAIPDEPTHS will not be parsed and analyzed since it cannot be requested efficiently { UUID1 : depth2, UUID2 : depth2 }
                "properties : { "+DAip.DAIPDEPTHS+" : { type : \"object\", enabled : false }, "+
                // DAIPPARENTS will be included but not tokenized [ UUID1, UUID2 ]
                DAip.DAIPPARENTS+" : { type : \"string\", index : \"not_analyzed\" }, "+
                // NBCHILD as the number of immediate child
                DAip.NBCHILD+" : { type : \"long\" },"+
                // Immediate parents will be included but not tokenized [ UUID1, UUID2 ]
                VitamLinks.DAip2DAip.field2to1+" : { type : \"string\", index : \"not_analyzed\" }, "+
                //"_id : { type : \"object\", enabled : false }, " +
                // All following items will neither be integrated neither analyzed
                VitamLinks.DAip2DAip.field1to2+" : { type : \"object\", enabled : false }, " +
                //VitamLinks.DAip2DAip.field2to1+" : { type : \"object\", enabled : false }, " +
                VitamLinks.Domain2DAip.field2to1+" : { type : \"object\", enabled : false }, " +
                VitamLinks.DAip2Dua.field1to2+" : { type : \"object\", enabled : false }, " +
                VitamLinks.DAip2PAip.field1to2+" : { type : \"object\", enabled : false } " +
                " } } }";
		LOGGER.debug("setMapping: "+indexName+" type: "+type+"\n\t"+mapping);
		try {
//        if (! client.admin().indices().prepareTypesExists(type).execute().actionGet().isExists()) {
            PutMappingResponse response = client.admin().indices().preparePutMapping().setIndices(indexName).setType(type)
                .setSource(mapping).execute().actionGet();
            LOGGER.info(type+":"+response.isAcknowledged());
            return response.isAcknowledged();
		} catch (Exception e) {
			LOGGER.error("Error while set Mapping", e);
			return false;
		}
//        }
        //System.err.println("not needed add Index");
//        return true;
    }
    /**
     * Add an entry in the ElasticSearch index
     * @param indexName
     * @param type
     * @param id
     * @param json
     * @return True if ok
     */
    public final boolean addEntryIndex(String indexName, String type, String id, String json) {
        client.prepareIndex(indexName, type, id)
                .setSource(json)
                .execute();
        return true;
    }
    /**
     * Add a set of entries in the ElasticSearch index
     * @param indexName
     * @param type
     * @param mapIdJson
     * @return True if ok
     */
    public final boolean addEntryIndexes(String indexName, String type, Map<String, String> mapIdJson) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        // either use client#prepare, or use Requests# to directly build index/delete requests
        for (Entry<String, String> val : mapIdJson.entrySet()) {
            bulkRequest.add(client.prepareIndex(indexName, type, val.getKey())
                    .setSource(val.getValue()));
        }
                
        bulkRequest.execute(); // new thread
        return true; //!bulkResponse.hasFailures();
        // Should process failures by iterating through each bulk response item
    }
    /**
     * Add a set of entries in the ElasticSearch index in blocking mode
     * @param indexName
     * @param type
     * @param mapIdJson
     * @return True if ok
     */
    public final boolean addEntryIndexesBlocking(String indexName, String type, Map<String, String> mapIdJson) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        // either use client#prepare, or use Requests# to directly build index/delete requests
        for (Entry<String, String> val : mapIdJson.entrySet()) {
            bulkRequest.add(client.prepareIndex(indexName, type, val.getKey())
                    .setSource(val.getValue()));
        }
                
        BulkResponse bulkResponse = bulkRequest.execute().actionGet(); // new thread
        return !bulkResponse.hasFailures();
        // Should process failures by iterating through each bulk response item
    }
    /**
     * Should be called only once saved (last time), but for the moment let the object as it is, next should remove not indexable entries
     * 
     * @param dbvitam
     * @param model
     * @param indexes
     * @param bson
     * @return the number of DAip incorporated (0 if none)
     */
	public static final int addEsIndex(MongoDbAccess dbvitam, String model, Map<String, String> indexes, BSONObject bson) {
		BasicDBObject maip = new BasicDBObject();
		maip.putAll(bson);
		maip.removeField(VitamLinks.DAip2DAip.field1to2);
		//Keep it maip.removeField(VitamLinks.DAip2DAip.field2to1);
		maip.removeField(VitamLinks.Domain2DAip.field2to1);
		maip.removeField(VitamLinks.DAip2Dua.field1to2);
		maip.removeField(VitamLinks.DAip2PAip.field1to2);
		String id = maip.getString(VitamType.ID);
		maip.removeField(VitamType.ID);
		//maip.removeField(ParserIngest.REFID);
		// DOMDEPTH already ok but duplicate it
		@SuppressWarnings("unchecked")
		HashMap<String, Integer> map = (HashMap<String, Integer>) maip.get(DAip.DAIPDEPTHS);
		List<String> list = new ArrayList<>();
		list.addAll(map.keySet());
		maip.append(DAip.DAIPPARENTS, list);
		//System.err.println(maip);
		//System.err.println(this);
		indexes.put(id, maip.toString());
		int nb = 0;
		if (indexes.size() > GlobalDatas.LIMIT_ES_NEW_INDEX) {
			nb = indexes.size();
			dbvitam.addEsEntryIndex(indexes, model);
			//dbvitam.flushOnDisk();
			indexes.clear();
			System.out.print(".");
		}
		maip.clear();
		maip = null;
		return nb;
	}

    /**
     * @param squery
     * @return the wrapped query
     */
    public static final QueryBuilder getQueryFromString(String squery) {
        return QueryBuilders.wrapperQuery(squery);
    }
    /**
     * 
     * @param sfilter
     * @return the wrapped filter
     */
    public static final FilterBuilder getFilterFromString(String sfilter) {
        return FilterBuilders.wrapperFilter(sfilter);
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
    public final ResultCached getSubDepth(String indexName, String type, String []currentNodes, int subdepth, QueryBuilder condition, FilterBuilder filterCond) {
        QueryBuilder query = null;
        FilterBuilder filter = null;
        if (GlobalDatas.useFilter) {
            filter = getSubDepthFilter(filterCond, currentNodes, subdepth);
            query = condition;
        } else {
            /*
             * filter where domdepths (currentNodes as (grand)parents, depth<=subdepth)
             */
        	QueryBuilder domdepths = null;
        	if (subdepth == 1) {
        		domdepths = QueryBuilders.termsQuery(VitamLinks.DAip2DAip.field2to1, currentNodes);
        	} else {
        		domdepths = QueryBuilders.termsQuery(DAip.DAIPPARENTS, currentNodes);
        	}
            /*QueryBuilder domdepths = null;
            if (subdepth == 1) {
                domdepths = QueryBuilders.multiMatchQuery(1, currentNodes);
            } else {
                if (currentNodes.length > 1) {
                    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
                    for (String node : currentNodes) {
                        boolQuery = boolQuery.should(QueryBuilders.rangeQuery(MetaAip.DOMDEPTHS+"."+node).lte(subdepth));
                    }
                    domdepths = boolQuery;
                } else {
                    domdepths = QueryBuilders.rangeQuery(MetaAip.DOMDEPTHS+"."+currentNodes[0]).lte(subdepth);
                }
            }*/
            /*
             * Condition query
             */
            query = QueryBuilders.boolQuery().must(domdepths).must(condition);
            filter = filterCond;
        }
        return search(indexName, type, query, filter, currentNodes, subdepth);
    }
    /**
     * Build the filter and facet filter for subdepth and currentNodes
     * @param filterCond
     * @param currentNodes
     * @param key
     * @param subdepth
     * @return the associated filter
     */
    private final FilterBuilder getSubDepthFilter(FilterBuilder filterCond, String []currentNodes, int subdepth) {
        /*
         * filter where domdepths (currentNodes as (grand)parents, depth<=subdepth)
         */
        FilterBuilder domdepths = null;
        TermsFilterBuilder filter = null;
        if (subdepth == 1) {
        	filter = FilterBuilders.termsFilter(VitamLinks.DAip2DAip.field2to1, currentNodes);
        } else {
        	filter = FilterBuilders.termsFilter(DAip.DAIPPARENTS, currentNodes);
        }
        if (filterCond != null) {
            domdepths = FilterBuilders.boolFilter()
                    .must(filter).must(filterCond);
        } else {
            domdepths = filter;
        }
        /*if (currentNodes.length > 1) {
            BoolFilterBuilder boolQuery = FilterBuilders.boolFilter();
            if (subdepth > 1) {
                for (String node : currentNodes) {
                    boolQuery = boolQuery.should(FilterBuilders.numericRangeFilter(MetaAip.DOMDEPTHS+"."+node).lte(subdepth));
                }
            } else {
                for (String node : currentNodes) {
                    boolQuery = boolQuery.should(FilterBuilders.termFilter(MetaAip.DOMDEPTHS+"."+node, subdepth));
                }
            }
            if (filterCond != null) {
                boolQuery = boolQuery.must(filterCond);
            } else if (GlobalDatas.useFilterCache) {
                boolQuery = boolQuery.cache(true).cacheKey(newkey);
            }
            domdepths = boolQuery;
        } else {
            if (subdepth > 1) {
                if (GlobalDatas.useFilterCache && filterCond == null) {
                    domdepths = 
                        FilterBuilders.numericRangeFilter(MetaAip.DOMDEPTHS+"."+currentNodes[0]).lte(subdepth)
                        .cache(true).cacheKey(newkey);
                } else {
                    domdepths = 
                            FilterBuilders.numericRangeFilter(MetaAip.DOMDEPTHS+"."+currentNodes[0]).lte(subdepth);
                }
            } else {
                if (GlobalDatas.useFilterCache && filterCond == null) {
                    domdepths = FilterBuilders.termFilter(MetaAip.DOMDEPTHS+"."+currentNodes[0], subdepth)
                            .cache(true).cacheKey(newkey);
                } else {
                    domdepths = FilterBuilders.termFilter(MetaAip.DOMDEPTHS+"."+currentNodes[0], subdepth);
                }
            }
            if (filterCond != null) {
                domdepths = FilterBuilders.boolFilter().must(filterCond).must(domdepths);
            }
        }*/
        return domdepths;
    }
    
    /**
     * 
     * @param indexName global index name (or split if needed)
     * @param type name of "1 model" within 1 global index
     * @param query as in DSL mode "{ "fieldname" : "value" }" "{ "match" : { "fieldname" : "value" } }" "{ "ids" : { "values" : [list of id] } }" 
     * @param filter
     * @param currentNodes
     * @param subdepth
     * @return a structure as ResultCached
     */
    protected final ResultCached search(String indexName, String type, 
            QueryBuilder query, FilterBuilder filter, String [] currentNodes, int subdepth) {
        SearchRequestBuilder request = client.prepareSearch(indexName)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setTypes(type)
                .setQuery(query)             // Query
                .setExplain(false)
                .setSize(GlobalDatas.limitLoad);
        if (filter != null) {
            request = request.setPostFilter(filter);   // Filter
        }
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("ESReq: {}", request);
        } else {
            LOGGER.debug("ESReq: {}", request);
        }
        SearchResponse response = request
                .execute()
                .actionGet();
        if (response.status() != RestStatus.OK) {
            LOGGER.error("Error "+response.status()+" from : "+request+":"+query+" # "+filter);
            return null;
        }
        SearchHits hits = response.getHits();
        if (hits.getTotalHits() > GlobalDatas.limitLoad) {
            LOGGER.warn("Warning, more than "+GlobalDatas.limitLoad+" hits: "+hits.getTotalHits());
        }
        if (hits.getTotalHits() == 0) {
            LOGGER.warn("No result from : "+request+":"+query+" # "+filter);
            return null;
        }
        long nb = 0;
        ResultCached resultRequest = new ResultCached();
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit hit = iterator.next();
            String id = hit.getId();
            Map<String, Object> src = hit.getSource();
            if (src != null) {
                Object val = src.get(DAip.NBCHILD);
                if (val == null) {
                    LOGGER.error("Not found "+DAip.NBCHILD);
                } else if (val instanceof Integer) {
                    nb += (Integer) val;
                } else {
                    LOGGER.error("Not Integer: "+val.getClass().getName());
                }
            }
            resultRequest.currentMaip.add(id);
        }
        resultRequest.nbSubNodes = nb;
        return resultRequest;
    }
    
}
