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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.BSONObject;
import org.elasticsearch.action.ListenableActionFuture;
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
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.IdsFilterBuilder;
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
 *
 * @author "Frederic Bregier"
 *
 */
public class ElasticSearchAccess {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(ElasticSearchAccess.class);

    private static void registerShutdownHook(final Node node) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                node.close();
            }
        });
    }

    static Node node;
    Node localNode;
    Client client;
    String clusterName;

    /**
     * Create an ElasticSearch access
     *
     * @param clusterName
     *            the name of the Cluster
     * @param unicast
     *            the unicast list of addresses
     * @param networkAddress
     *            the local network address (if any)
     */
    public ElasticSearchAccess(final String clusterName, final String unicast, final String networkAddress) {
        Settings settings = null;
        this.clusterName = clusterName;
        if (networkAddress != null) {
            settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName)
                    .put("discovery.zen.ping.multicast.enabled", false).put("discovery.zen.ping.unicast.hosts", unicast)
                    .put("http.enabled", false)
                    .put("index.merge.async", true)
                    .put("network.host", networkAddress)
                    .build();
        } else {
            settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName)
                    .put("discovery.zen.ping.multicast.enabled", false).put("discovery.zen.ping.unicast.hosts", unicast)
                    .put("http.enabled", false)
                    .put("index.merge.async", true)
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
     * 
     * @return the Cluster Name
     */
    public String getClusterName() {
        return clusterName;
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
     *
     * @param idxName
     * @return True if ok
     */
    public final boolean deleteIndex(final String idxName) {
        try {
            if (client.admin().indices().prepareExists(idxName).execute().actionGet().isExists()) {
                if (!client.admin().indices().prepareDelete(idxName).execute().actionGet().isAcknowledged()) {
                    LOGGER.error("Error on index delete");
                }
            }
            return true;
        } catch (final Exception e) {
            LOGGER.error("Error while deleting index", e);
            return true;
        }
    }

    /**
     * Add a type to an index
     *
     * @param indexName
     * @param type
     * @return True if ok
     */
    public final boolean addIndex(final String indexName, final String type) {
        LOGGER.debug("addIndex");
        if (!client.admin().indices().prepareExists(indexName).execute().actionGet().isExists()) {
            LOGGER.debug("createIndex");
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
        }
        if (type == null) {
            return false;
        }
        final String mapping = "{"
                + type
                +
                // Will keep DAIPDEPTHS and NBCHILD as value to get (_id is implicit)
                // Change since DAIPDEPTHS not useful as source
                // " : { _source : { includes : [\""+DAip.DAIPDEPTHS+".*\", \""+DAip.NBCHILD+"\"] },"+
                " : { _source : { includes : [\"" 
                + DAip.NBCHILD + "\"] },"
                +
                // DAIPDEPTHS will not be parsed and analyzed since it cannot be requested efficiently { UUID1 : depth2, UUID2 :
                // depth2 }
                "properties : { "
                + DAip.DAIPDEPTHS + " : { type : \"object\", enabled : false }, "
                +
                // DAIPPARENTS will be included but not tokenized [ UUID1, UUID2 ]
                DAip.DAIPPARENTS + " : { type : \"string\", index : \"not_analyzed\" }, "
                +
                // NBCHILD as the number of immediate child
                DAip.NBCHILD + " : { type : \"long\" },"
                +
                // Immediate parents will be included but not tokenized [ UUID1, UUID2 ]
                VitamLinks.DAip2DAip.field2to1 + " : { type : \"string\", index : \"not_analyzed\" }, "
                +
                // "_id : { type : \"object\", enabled : false }, " +
                // This item does not exist and therefore should not be analyzed neither exist
                VitamLinks.DAip2DAip.field1to2 + " : { type : \"object\", enabled : false }, "
                +
                // VitamLinks.DAip2DAip.field2to1+" : { type : \"object\", enabled : false }, " +
                VitamLinks.Domain2DAip.field2to1 + " : { type : \"string\", index : \"not_analyzed\" }, " + 
                VitamLinks.DAip2Dua.field1to2 + " : { type : \"string\", index : \"not_analyzed\" }, " + 
                VitamLinks.DAip2PAip.field1to2 + " : { type : \"string\", index : \"not_analyzed\" } " +
                " } } }";
        LOGGER.debug("setMapping: " + indexName + " type: " + type + "\n\t" + mapping);
        try {
            // if (! client.admin().indices().prepareTypesExists(type).execute().actionGet().isExists()) {
            final PutMappingResponse response = client.admin().indices().preparePutMapping().setIndices(indexName).setType(type)
                    .setSource(mapping).execute().actionGet();
            LOGGER.info(type + ":" + response.isAcknowledged());
            return response.isAcknowledged();
        } catch (final Exception e) {
            LOGGER.error("Error while set Mapping", e);
            return false;
        }
        // }
        // System.err.println("not needed add Index");
        // return true;
    }

    /**
     * Add an entry in the ElasticSearch index
     *
     * @param indexName
     * @param type
     * @param id
     * @param json
     * @return True if ok
     */
    public final boolean addEntryIndex(final String indexName, final String type, final String id, final String json) {
        return client.prepareIndex(indexName, type, id).setSource(json).execute().actionGet().getVersion() > 0;
    }

    private final ListenableActionFuture<BulkResponse> addEntryIndexesInternal(final String indexName, final String type, final Map<String, String> mapIdJson) {
        final BulkRequestBuilder bulkRequest = client.prepareBulk();
        bulkRequest.setRefresh(false);
        // either use client#prepare, or use Requests# to directly build index/delete requests
        for (final Entry<String, String> val : mapIdJson.entrySet()) {
            bulkRequest.add(client.prepareIndex(indexName, type, val.getKey()).setSource(val.getValue()));
        }

        return bulkRequest.execute(); // new thread
    }
    /**
     * Add a set of entries in the ElasticSearch index
     *
     * @param indexName
     * @param type
     * @param mapIdJson
     * @return True if ok
     */
    public final boolean addEntryIndexes(final String indexName, final String type, final Map<String, String> mapIdJson) {
        addEntryIndexesInternal(indexName, type, mapIdJson);
        return true;
    }

    /**
     * Add a set of entries in the ElasticSearch index in blocking mode
     *
     * @param indexName
     * @param type
     * @param mapIdJson
     * @return True if ok
     */
    public final boolean addEntryIndexesBlocking(final String indexName, final String type, final Map<String, String> mapIdJson) {
        final BulkResponse bulkResponse = addEntryIndexesInternal(indexName, type, mapIdJson).actionGet();
        return !bulkResponse.hasFailures();
        // Should process failures by iterating through each bulk response item
    }
    
    private static final BasicDBObject getFiltered(final BSONObject bson) {
        BasicDBObject maip = new BasicDBObject();
        maip.putAll(bson);
        maip.removeField(VitamLinks.DAip2DAip.field1to2);
        // Keep it maip.removeField(VitamLinks.DAip2DAip.field2to1);
        //maip.removeField(VitamLinks.Domain2DAip.field2to1);
        //maip.removeField(VitamLinks.DAip2Dua.field1to2);
        //maip.removeField(VitamLinks.DAip2PAip.field1to2);
        // maip.removeField(ParserIngest.REFID);
        // DOMDEPTH already ok but duplicate it
        @SuppressWarnings("unchecked")
        final HashMap<String, Integer> map = (HashMap<String, Integer>) maip.get(DAip.DAIPDEPTHS);
        //final List<String> list = new ArrayList<>(map.keySet());
        maip.append(DAip.DAIPPARENTS, map.keySet());// was list);
        return maip;
    }
    /**
     * 
     * @param dbvitam
     * @param model
     * @param bson
     * @return True if inserted in ES
     */
    public static final boolean addEsIndex(final MongoDbAccess dbvitam, final String model,
            final BSONObject bson) {
        BasicDBObject maip = getFiltered(bson);
        final String id = maip.getString(VitamType.ID);
        maip.removeField(VitamType.ID);
        return dbvitam.addEsEntryIndex(model, id, maip.toString());
    }

    /**
     * Should be called only once saved (last time), but for the moment let the object as it is, next should remove not indexable
     * entries
     *
     * @param dbvitam
     * @param model
     * @param indexes
     * @param bson
     * @return the number of DAip incorporated (0 if none)
     */
    public static final int addEsIndex(final MongoDbAccess dbvitam, final String model, final Map<String, String> indexes,
            final BSONObject bson) {
        BasicDBObject maip = getFiltered(bson);
        final String id = maip.getString(VitamType.ID);
        maip.removeField(VitamType.ID);
        // System.err.println(maip);
        // System.err.println(this);
        indexes.put(id, maip.toString());
        int nb = 0;
        if (indexes.size() > GlobalDatas.LIMIT_ES_NEW_INDEX) {
            nb = indexes.size();
            dbvitam.addEsEntryIndex(indexes, model);
            // dbvitam.flushOnDisk();
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
    public static final QueryBuilder getQueryFromString(final String squery) {
        return QueryBuilders.wrapperQuery(squery);
    }

    /**
     *
     * @param sfilter
     * @return the wrapped filter
     */
    public static final FilterBuilder getFilterFromString(final String sfilter) {
        return FilterBuilders.wrapperFilter(sfilter);
    }

    /**
     *
     * @param indexName
     * @param type
     * @param currentNodes
     *            current parent nodes
     * @param subdepth
     * @param condition
     * @param filterCond
     * @return the ResultInterface associated with this request. 
     *         Note that the exact depth is not checked, so it must be checked
     *         after (using checkAncestor method)
     */
    public final ResultInterface getSubDepth(final String indexName, final String type, final String[] currentNodes,
            final int subdepth, final QueryBuilder condition, final FilterBuilder filterCond) {
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
                domdepths = QueryBuilders.boolQuery()
                        .should(QueryBuilders.termsQuery(VitamLinks.DAip2DAip.field2to1, currentNodes))
                        .should(QueryBuilders.termsQuery(VitamLinks.Domain2DAip.field2to1, currentNodes));
            } else {
                domdepths = QueryBuilders.termsQuery(DAip.DAIPPARENTS, currentNodes);
            }
            /*
             * QueryBuilder domdepths = null;
             * if (subdepth == 1) {
             * domdepths = QueryBuilders.multiMatchQuery(1, currentNodes);
             * } else {
             * if (currentNodes.length > 1) {
             * BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
             * for (String node : currentNodes) {
             * boolQuery = boolQuery.should(QueryBuilders.rangeQuery(MetaAip.DOMDEPTHS+"."+node).lte(subdepth));
             * }
             * domdepths = boolQuery;
             * } else {
             * domdepths = QueryBuilders.rangeQuery(MetaAip.DOMDEPTHS+"."+currentNodes[0]).lte(subdepth);
             * }
             * }
             */
            /*
             * Condition query
             */
            query = QueryBuilders.boolQuery().must(domdepths).must(condition);
            filter = filterCond;
        }
        return search(indexName, type, query, filter);
    }

    /**
     * Build the filter and facet filter for subdepth and currentNodes
     *
     * @param filterCond
     * @param currentNodes
     * @param key
     * @param subdepth
     * @return the associated filter
     */
    private final FilterBuilder getSubDepthFilter(final FilterBuilder filterCond, final String[] currentNodes, final int subdepth) {
        /*
         * filter where domdepths (currentNodes as (grand)parents, depth<=subdepth)
         */
        FilterBuilder domdepths = null;
        FilterBuilder filter = null;
        if (subdepth == 1) {
            filter = FilterBuilders.boolFilter()
                    .should(FilterBuilders.termsFilter(VitamLinks.DAip2DAip.field2to1, currentNodes))
                    .should(FilterBuilders.termsFilter(VitamLinks.Domain2DAip.field2to1, currentNodes));
            if (GlobalDatas.PRINT_REQUEST) {
                LOGGER.warn("Filter: terms {} or {} = {}", VitamLinks.DAip2DAip.field2to1, 
                        VitamLinks.Domain2DAip.field2to1, currentNodes);
            }
        } else {
            filter = FilterBuilders.termsFilter(DAip.DAIPPARENTS, currentNodes);
            if (GlobalDatas.PRINT_REQUEST) {
                LOGGER.warn("ESReq: terms {} = {}", DAip.DAIPPARENTS, currentNodes);
            }
        }
        if (filterCond != null) {
            domdepths = FilterBuilders.boolFilter().must(filter).must(filterCond);
        } else {
            domdepths = filter;
        }
        /*
         * if (currentNodes.length > 1) {
         * BoolFilterBuilder boolQuery = FilterBuilders.boolFilter();
         * if (subdepth > 1) {
         * for (String node : currentNodes) {
         * boolQuery = boolQuery.should(FilterBuilders.numericRangeFilter(MetaAip.DOMDEPTHS+"."+node).lte(subdepth));
         * }
         * } else {
         * for (String node : currentNodes) {
         * boolQuery = boolQuery.should(FilterBuilders.termFilter(MetaAip.DOMDEPTHS+"."+node, subdepth));
         * }
         * }
         * if (filterCond != null) {
         * boolQuery = boolQuery.must(filterCond);
         * } else if (GlobalDatas.useFilterCache) {
         * boolQuery = boolQuery.cache(true).cacheKey(newkey);
         * }
         * domdepths = boolQuery;
         * } else {
         * if (subdepth > 1) {
         * if (GlobalDatas.useFilterCache && filterCond == null) {
         * domdepths =
         * FilterBuilders.numericRangeFilter(MetaAip.DOMDEPTHS+"."+currentNodes[0]).lte(subdepth)
         * .cache(true).cacheKey(newkey);
         * } else {
         * domdepths =
         * FilterBuilders.numericRangeFilter(MetaAip.DOMDEPTHS+"."+currentNodes[0]).lte(subdepth);
         * }
         * } else {
         * if (GlobalDatas.useFilterCache && filterCond == null) {
         * domdepths = FilterBuilders.termFilter(MetaAip.DOMDEPTHS+"."+currentNodes[0], subdepth)
         * .cache(true).cacheKey(newkey);
         * } else {
         * domdepths = FilterBuilders.termFilter(MetaAip.DOMDEPTHS+"."+currentNodes[0], subdepth);
         * }
         * }
         * if (filterCond != null) {
         * domdepths = FilterBuilders.boolFilter().must(filterCond).must(domdepths);
         * }
         * }
         */
        return domdepths;
    }

    /**
    *
    * @param indexName
    * @param type
    * @param currentNodes
    *            current parent nodes
    * @param subdepth
    * @param condition
    * @param filterCond
    * @return the ResultInterface associated with this request. 
    *         Note that the exact depth is not checked, so it must be checked
    *         after (using checkAncestor method)
    */
   public final ResultInterface getSubDepthStart(final String indexName, final String type, final String[] currentNodes,
           final int subdepth, final QueryBuilder condition, final FilterBuilder filterCond) {
       QueryBuilder query = null;
       FilterBuilder filter = null;
       if (GlobalDatas.useFilter) {
           filter = getSubDepthFilterStart(filterCond, currentNodes, subdepth);
           query = condition;
       } else {
           /*
            * filter where domdepths (currentNodes as list of ids)
            */
           QueryBuilder domdepths = QueryBuilders.idsQuery(currentNodes);
           /*
            * Condition query
            */
           query = QueryBuilders.boolQuery().must(domdepths).must(condition);
           filter = filterCond;
       }
       return search(indexName, type, query, filter);
   }

   /**
    * Build the filter and facet filter for subdepth and currentNodes
    *
    * @param filterCond
    * @param currentNodes
    * @param key
    * @param subdepth
    * @return the associated filter
    */
   private final FilterBuilder getSubDepthFilterStart(final FilterBuilder filterCond, final String[] currentNodes, final int subdepth) {
       /*
        * filter where domdepths (currentNodes as list of ids)
        */
       FilterBuilder domdepths = null;
       IdsFilterBuilder filter = FilterBuilders.idsFilter(currentNodes);
       if (filterCond != null) {
           domdepths = FilterBuilders.boolFilter().must(filter).must(filterCond);
       } else {
           domdepths = filter;
       }
       return domdepths;
   }

    /**
    *
    * @param indexName
    * @param type
    * @param subset subset of valid nodes
    * @param condition
    * @param filterCond
    * @return the ResultInterface associated with this request. 
    */
   public final ResultInterface getNegativeSubDepth(final String indexName, final String type, final String[] subset,
           final QueryBuilder condition, final FilterBuilder filterCond) {
       QueryBuilder query = null;
       FilterBuilder filter = null;
       
       if (GlobalDatas.useFilter) {
           /*
            * filter where id from subset
            */
           TermsFilterBuilder filterTerms = null;
           filterTerms = FilterBuilders.termsFilter(DAip.ID, subset);
           if (filterCond != null) {
               filter = FilterBuilders.boolFilter().must(filterTerms).must(filterCond);
           } else {
               filter = filterTerms;
           }
           query = condition;
       } else {
           /*
            * filter where id from subset
            */
           QueryBuilder domdepths = null;
           domdepths = QueryBuilders.termsQuery(DAip.ID, subset);
           /*
            * Condition query
            */
           query = QueryBuilders.boolQuery().must(domdepths).must(condition);
           filter = filterCond;
       }
       return search(indexName, type, query, filter);
   }

   /**
     *
     * @param indexName
     *            global index name (or split if needed)
     * @param type
     *            name of "1 model" within 1 global index
     * @param query
     *            as in DSL mode "{ "fieldname" : "value" }" "{ "match" : { "fieldname" : "value" } }"
     *            "{ "ids" : { "values" : [list of id] } }"
     * @param filter
     * @return a structure as ResultInterface
     */
    protected final ResultInterface search(final String indexName, final String type, final QueryBuilder query,
            final FilterBuilder filter) {
        SearchRequestBuilder request = client.prepareSearch(indexName).setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setTypes(type).setExplain(false).setSize(GlobalDatas.limitLoad);
        if (filter != null) {
            if (GlobalDatas.useFilteredRequest) {
                FilteredQueryBuilder filteredQueryBuilder = QueryBuilders.filteredQuery(query, filter);
                request.setQuery(filteredQueryBuilder);
            } else {
                request.setQuery(query).setPostFilter(filter);
            }
        } else {
            request.setQuery(query);
        }
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("ESReq: {}", request);
        } else {
            LOGGER.debug("ESReq: {}", request);
        }
        final SearchResponse response = request.execute().actionGet();
        if (response.status() != RestStatus.OK) {
            LOGGER.error("Error " + response.status() + " from : " + request + ":" + query + " # " + filter);
            return null;
        }
        final SearchHits hits = response.getHits();
        if (hits.getTotalHits() > GlobalDatas.limitLoad) {
            LOGGER.warn("Warning, more than " + GlobalDatas.limitLoad + " hits: " + hits.getTotalHits());
        }
        if (hits.getTotalHits() == 0) {
            LOGGER.info("No result from : " + request);
            return null;
        }
        long nb = 0;
        final ResultInterface resultRequest = MongoDbAccess.createOneResult();
        final Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            final SearchHit hit = iterator.next();
            final String id = hit.getId();
            final Map<String, Object> src = hit.getSource();
            if (src != null) {
                final Object val = src.get(DAip.NBCHILD);
                if (val == null) {
                    LOGGER.error("Not found " + DAip.NBCHILD);
                } else if (val instanceof Integer) {
                    nb += (Integer) val;
                    if (GlobalDatas.PRINT_REQUEST) {
                        LOGGER.warn("Result: {} : {}", id, val);
                    }
                } else {
                    LOGGER.error("Not Integer: " + val.getClass().getName());
                }
            }
            resultRequest.getCurrentDaip().add(id);
        }
        resultRequest.setNbSubNodes(nb);
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("FinalEsResult: {} : {}", resultRequest.getCurrentDaip(), resultRequest.getNbSubNodes());
        }
        return resultRequest;
    }

}
