/**
 * This file is part of Vitam Project.
 *
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 *
 * All Vitam Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Vitam is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with Vitam . If not, see
 * <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.query.parser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.mdbes.VitamType;
import fr.gouv.vitam.query.exception.InvalidParseOperationException;
import fr.gouv.vitam.query.json.JsonHandler;
import fr.gouv.vitam.query.parser.ParserTokens.FILTERARGS;
import fr.gouv.vitam.query.parser.ParserTokens.GLOBAL;
import fr.gouv.vitam.query.parser.ParserTokens.PROJECTION;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;
import fr.gouv.vitam.query.parser.ParserTokens.REQUESTARGS;
import fr.gouv.vitam.query.parser.ParserTokens.REQUESTFILTER;
import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * @author "Frederic Bregier"
 *
 */
public abstract class AbstractQueryParser {
    /**
     * Front part for ES attribute not parsed
     */
    public static final String _NA = "_na_";

    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(AbstractQueryParser.class);

    /**
     * Default position of MongoDB element
     */
    public static final int MONGODB = 0;
    /**
     * Default position of ElasticSearch element
     */
    public static final int ELASTICSEARCH = 1;

    /**
     * ElasticSearch Keywords
     *
     */
    @SuppressWarnings("javadoc")
    public static enum ES_KEYWORDS {
        range, like_text, simple_query_string, query, fields, regexp, term, wildcard, 
        field, bool, must_not, should, must, missing, existence, null_value, script, max_expansions
    }

    protected boolean usingMongoDb = false;
    protected boolean usingCouchBase = false;
    protected boolean usingElasticSearch = false;

    protected String request;

    protected List<String> sources = new ArrayList<String>();
    private final List<TypeRequest> requests = new ArrayList<TypeRequest>();
    protected long limit = 0;
    protected long offset = 0;
    protected ObjectNode orderBy = null;
    protected ObjectNode projection = null;
    protected String contractId;
    protected boolean hintCache = false;

    protected boolean simulate = false;
    protected int lastDepth = 0;
    protected int nbModel;

    /**
     * @param simul
     */
    public AbstractQueryParser(final boolean simul) {
        simulate = simul;
        nbModel = 2;
    }

    /**
     * @param simul
     */
    public void setSimulate(final boolean simul) {
        simulate = simul;
    }

    /**
     * To be implemented correctly, according to specific attribute not analyzed (as "id" or attributes
     * starting with "_na_")
     *
     * @param attributeName
     * @return True if this attribute is not analyzed by ElasticSearch, else False
     */
    public boolean isAttributeNotAnalyzed(final String attributeName) {
        return (attributeName.startsWith(_NA) || VitamType.ID.equals(attributeName));
    }

    /**
     *
     * @param request
     *            containing a JSON as [ {query}, {filter}, {projection} ] or { query : select, filter : filter, projection :
     *            projection }
     * @throws InvalidParseOperationException
     */
    public void parse(final String request) throws InvalidParseOperationException {
        this.request = request;
        final JsonNode rootNode = JsonHandler.getFromString(request);
        if (rootNode.isMissingNode()) {
            throw new InvalidParseOperationException("The current Node is missing(empty): RequestRoot");
        }
        if (rootNode.isArray()) {
            // should be 3, but each could be empty ( '{}' )
            queryParse(rootNode.get(0));
            filterParse(rootNode.get(1));
            projectionParse(rootNode.get(2));
        } else {
            // not as array but composite as { $query : query, $filter : filter, $projection : projection }
            queryParse(rootNode.get(GLOBAL.query.exactToken()));
            filterParse(rootNode.get(GLOBAL.filter.exactToken()));
            projectionParse(rootNode.get(GLOBAL.projection.exactToken()));
        }
    }
    /**
     * 
     * @param request containing only the JSON query part (no filter neither projection)
     * @throws InvalidParseOperationException
     */
    public void parseQueryOnly(final String request) throws InvalidParseOperationException {
        this.request = request;
        final JsonNode rootNode = JsonHandler.getFromString(request);
        if (rootNode.isMissingNode()) {
            throw new InvalidParseOperationException("The current Node is missing(empty): RequestRoot");
        }
        // Not as array and no filter no projection
        queryParse(rootNode);
        filterParse(JsonHandler.createObjectNode());
        projectionParse(JsonHandler.createObjectNode());
    }
    /**
     * In MongoDB : find(Query, Projection).sort(SortFilter).skip(SkipFilter).limit(LimitFilter);
     * In addition, one shall limit the scan by: find(Query, Projection)._addSpecial( "$maxscan", highlimit
     * ).sort(SortFilter).skip(SkipFilter).limit(LimitFilter);
     *
     * In ElasticSearch :
     * Query => { "from" : offset, "size" : number, "sort" : [ SortFilter as "name" : { "order" : "asc|desc" } ], "query" : Query
     * }
     * FilteredQuery => { "filtered" : { "query" : { Query }, "filter" : { "limit" : { "value" : limit } } } }
     */
    protected void filterParse(final JsonNode rootNode) throws InvalidParseOperationException {
        hintCache = false;
        limit = 0;
        offset = 0;
        orderBy = null;
        try {
            if (rootNode.has(REQUESTFILTER.hint.exactToken())) {
                // $hint : [ cache/nocache, ... ]
                final JsonNode node = rootNode.get(REQUESTFILTER.hint.exactToken());
                if (node.isArray()) {
                    final ArrayNode array = (ArrayNode) node;
                    for (final JsonNode jsonNode : array) {
                        if (jsonNode.asText().equalsIgnoreCase(FILTERARGS.cache.exactToken())) {
                            hintCache = true;
                        }
                    }
                } else {
                    if (node.asText().equalsIgnoreCase(FILTERARGS.cache.exactToken())) {
                        hintCache = true;
                    }
                }
            }
            if (rootNode.has(REQUESTFILTER.limit.exactToken())) {
                /*
                 * $limit : n
                 * $maxScan: <number> / cursor.limit(n)
                 * "filter" : { "limit" : {"value" : n} } ou "from" : start, "size" : n
                 */
                limit = rootNode.get(REQUESTFILTER.limit.exactToken()).asLong();
            }
            if (rootNode.has(REQUESTFILTER.offset.exactToken())) {
                /*
                 * $offset : start
                 * cursor.skip(start)
                 * "from" : start, "size" : n
                 */
                offset = rootNode.get(REQUESTFILTER.offset.exactToken()).asLong();
            }
            if (rootNode.has(REQUESTFILTER.orderby.exactToken())) {
                /*
                 * $orderby : { key : +/-1, ... }
                 * $orderby: { key : +/-1, ... }
                 * "sort" : [ { "key" : "asc/desc"}, ..., "_score" ]
                 */
                orderBy = JsonHandler.createObjectNode();
                final JsonNode node = rootNode.get(REQUESTFILTER.orderby.exactToken());
                if (node.isArray()) {
                    for (final JsonNode jsonNode : node) {
                        final Entry<String, JsonNode> entry = JsonHandler.checkLaxUnicity("OrderByArrayEntry", jsonNode);
                        if (entry.getKey() != null) {
                            if (entry.getValue().isNull()) {
                                orderBy.put(entry.getKey(), 1);
                            } else {
                                orderBy.set(entry.getKey(), entry.getValue());
                            }
                        } else {
                            orderBy.put(entry.getValue().asText(), 1);
                        }
                    }
                } else {
                    for (final Iterator<Entry<String, JsonNode>> iterator = node.fields(); iterator.hasNext();) {
                        final Entry<String, JsonNode> entry = iterator.next();
                        if (entry.getValue().isNull()) {
                            orderBy.put(entry.getKey(), 1);
                        } else {
                            orderBy.set(entry.getKey(), entry.getValue());
                        }
                    }
                }
            }
        } catch (final Exception e) {
            throw new InvalidParseOperationException("Parse in error for Filter: " + rootNode, e);
        }
    }

    /**
     * 
     * @return True if the cache hint is in the query
     */
    public boolean hintCache() {
        return hintCache;
    }
    /**
     * $fields : {name1 : 0/1, name2 : 0/1, ...}, $usage : contractId
     *
     * @param rootNode
     * @throws InvalidParseOperationException
     */
    protected void projectionParse(final JsonNode rootNode) throws InvalidParseOperationException {
        projection = null;
        contractId = null;
        try {
            if (rootNode.has(PROJECTION.fields.exactToken())) {
                /*
                 * $fields : {name1 : 0/1, name2 : 0/1, ...}
                 * {name1 : 0/1, name2 : 0/1}
                 * ES: none since result will come out of MD
                 */
                final JsonNode node = rootNode.get(PROJECTION.fields.exactToken());
                if (node.isArray()) {
                    projection = JsonHandler.createObjectNode();
                    for (final JsonNode jsonNode : node) {
                        projection.setAll((ObjectNode) jsonNode);
                    }
                } else {
                    projection = (ObjectNode) rootNode.get(PROJECTION.fields.exactToken());
                }
            }
            if (rootNode.has(PROJECTION.usage.exactToken())) {
                contractId = rootNode.get(PROJECTION.usage.exactToken()).asText();
            }
        } catch (final Exception e) {
            throw new InvalidParseOperationException("Parse in error for Projection: " + rootNode, e);
        }
    }

    /**
     * [ query, query ] or { query } if one level only
     *
     * @param rootNode
     * @throws InvalidParseOperationException
     */
    protected void queryParse(final JsonNode rootNode) throws InvalidParseOperationException {
        try {
            if (rootNode.isArray()) {
                // level are described as array entries, each being single element (no name)
                for (final JsonNode level : rootNode) {
                    // now parse sub element as single command/value
                    analyzeRootRequest(level);
                }
            } else {
                // 1 level only: might have 2 fields (request, depth)
                analyzeRootRequest(rootNode);
            }
        } catch (final Exception e) {
            throw new InvalidParseOperationException("Parse in error for Query: " + rootNode, e);
        }
    }

    /**
     * { expression, $depth : exactdepth, $relativedepth : /- depth }, $depth and $relativedepth being optional (mutual exclusive)
     *
     * @param command
     * @throws InvalidParseOperationException
     */
    protected void analyzeRootRequest(final JsonNode command) throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed");
        }
        sources.add(command.toString());
        int relativedepth = 1; // default is immediate next level
        int exactdepth = 0; // default is to not specify any exact depth (implicit)
        boolean isDepth = false;
        // first verify if depth is set
        if (command.has(REQUESTARGS.depth.exactToken())) {
            final JsonNode jdepth = ((ObjectNode) command).remove(REQUESTARGS.depth.exactToken());
            if (jdepth != null) {
                exactdepth = jdepth.asInt();
                isDepth = true;
            }
            ((ObjectNode) command).remove(REQUESTARGS.relativedepth.exactToken());
        } else if (command.has(REQUESTARGS.relativedepth.exactToken())) {
            final JsonNode jdepth = ((ObjectNode) command).remove(REQUESTARGS.relativedepth.exactToken());
            if (jdepth != null) {
                relativedepth = jdepth.asInt();
                if (relativedepth == 0) {
                    relativedepth = GlobalDatas.MAXDEPTH;
                }
                isDepth = true;
            }
        }
        // now single element
        final Entry<String, JsonNode> requestItem = JsonHandler.checkUnicity("RootRequest", command);
        TypeRequest tr = null;
        if (requestItem.getKey().equalsIgnoreCase(REQUEST.path.exactToken())) {
            if (isDepth) {
                throw new InvalidParseOperationException("Invalid combined command Depth and Path: " + command);
            }
            final int prevDepth = lastDepth;
            tr = analyzePath(requestItem.getKey(), requestItem.getValue());
            LOGGER.debug("Depth step: {}:{}", lastDepth, lastDepth - prevDepth);
        } else {
            tr = analyzeOneCommand(requestItem.getKey(), requestItem.getValue());
            tr.relativedepth = relativedepth;
            tr.exactdepth = exactdepth;
            tr.isDepth = isDepth;
            final int prevDepth = lastDepth;
            if (exactdepth > 0) {
                lastDepth = exactdepth;
            } else if (relativedepth != 0) {
                lastDepth += relativedepth;
            }
            LOGGER.debug("Depth step: {}:{}:{}:{}:{}", lastDepth, lastDepth - prevDepth, relativedepth, exactdepth, isDepth);
            checkRootTypeRequest(tr, command, prevDepth);
        }
        getRequests().add(tr);
    }

    protected void checkRootTypeRequest(final TypeRequest tr, final JsonNode command, final int prevDepth)
            throws InvalidParseOperationException {
        if (lastDepth < 0 || (lastDepth <= 1 && tr.relativedepth < 0)) {
            throw new InvalidParseOperationException("Depth operation is not correct since final level might be negative: "+
                    lastDepth+" or up to 1 but using negative relative depth: "+tr.relativedepth);
        }
        if (tr.isOnlyES || tr.relativedepth > 1 || lastDepth - prevDepth > 1) {
            // MongoDB not allowed
            tr.isOnlyES = true;
            LOGGER.debug("ES only: {}", command);
        }
    }

    protected static final void unionTransaction(final TypeRequest tr0, final TypeRequest tr) {
        tr0.isOnlyES |= tr.isOnlyES;
    }

    /**
     * $path : [ id1, id2, ... ]
     *
     * @param refCommand
     * @param command
     * @return the corresponding TypeRequest
     * @throws InvalidParseOperationException
     */
    protected final TypeRequest analyzePath(final String refCommand, final JsonNode command)
            throws InvalidParseOperationException {
        final TypeRequest tr0 = new TypeRequest(nbModel);
        final REQUEST req = getRequestId(refCommand);
        tr0.type = req;
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        tr0.refId = new ArrayList<>(command.size());
        lastDepth += command.size();
        for (final JsonNode jsonNode : command) {
            tr0.refId.add(jsonNode.asText());
        }
        return tr0;
    }

    protected static final REQUEST getRequestId(final String reqroot) throws InvalidParseOperationException {
        if (!reqroot.startsWith("$")) {
            throw new InvalidParseOperationException("Incorrect query $command: " + reqroot);
        }
        final String command = reqroot.substring(1);
        REQUEST req = null;
        try {
            req = REQUEST.valueOf(command);
        } catch (final IllegalArgumentException e) {
            throw new InvalidParseOperationException("Invalid query command: " + command, e);
        }
        return req;
    }

    protected TypeRequest analyzeOneCommand(final String refCommand, final JsonNode command)
            throws InvalidParseOperationException {
        final TypeRequest tr0 = new TypeRequest(nbModel);
        final REQUEST req = getRequestId(refCommand);
        tr0.type = req;
        switch (req) {
            case and:
            case not:
            case nor:
            case or: {
                analyzeAndNotNorOr(refCommand, command, tr0, req);
                break;
            }
            case exists:
            case missing: {
                analyzeExistsMissing(refCommand, command, tr0, req);
                break;
            }
            case flt:
            case mlt: {
                analyzeXlt(refCommand, command, tr0, req);
                break;
            }
            case match:
            case match_phrase:
            case match_phrase_prefix:
            case prefix: {
                analyzeMatch(refCommand, command, tr0, req);
                break;
            }
            case nin:
            case in: {
                analyzeIn(refCommand, command, tr0, req);
                break;
            }
            case range: {
                analyzeRange(refCommand, command, tr0, req);
                break;
            }
            case regex: {
                analyzeRegex(refCommand, command, tr0, req);
                break;
            }
            case term: {
                analyzeTerm(refCommand, command, tr0, req);
                break;
            }
            case wildcard: {
                analyzeWildcard(refCommand, command, tr0, req);
                break;
            }
            case eq:
            case ne: {
                analyzeEq(refCommand, command, tr0, req);
                break;
            }
            case gt:
            case gte:
            case lt:
            case lte: {
                analyzeCompare(refCommand, command, tr0, req);
                break;
            }
            case search: {
                analyzeSearch(refCommand, command, tr0, req);
                break;
            }
            case isNull: {
                analyzeIsNull(refCommand, command, tr0, req);
                break;
            }
            case size: {
                analyzeSize(refCommand, command, tr0, req);
                break;
            }
            case geometry:
            case box:
            case polygon:
            case center:
            case geoIntersects:
            case geoWithin:
            case near: {
                throw new InvalidParseOperationException("Unimplemented command: " + refCommand);
            }
            case path: {
                throw new InvalidParseOperationException("Invalid position for command: " + refCommand);
            }
            default:
                throw new InvalidParseOperationException("Invalid command: " + refCommand);
        }
        return tr0;
    }

    /**
     * $size : { name : length }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected abstract void analyzeSize(String refCommand, JsonNode command, TypeRequest tr0, REQUEST req)
            throws InvalidParseOperationException;

    /**
     * $gt : { name : value }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected abstract void analyzeCompare(String refCommand, JsonNode command, TypeRequest tr0, REQUEST req)
            throws InvalidParseOperationException;

    /**
     * $flt : { $fields : [ name1, name2 ], $like : like_text }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected abstract void analyzeXlt(String refCommand, JsonNode command, TypeRequest tr0, REQUEST req)
            throws InvalidParseOperationException;

    /**
     * $search : { name : searchParameter }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected abstract void analyzeSearch(String refCommand, JsonNode command, TypeRequest tr0, REQUEST req)
            throws InvalidParseOperationException;

    /**
     * $match : { name : words, $max_expansions : n }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected abstract void analyzeMatch(String refCommand, JsonNode command, TypeRequest tr0, REQUEST req)
            throws InvalidParseOperationException;

    /**
     * $in : { name : [ value1, value2, ... ] }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @throws InvalidParseOperationException
     */
    protected abstract void analyzeIn(String refCommand, JsonNode command, TypeRequest tr0, REQUEST req)
            throws InvalidParseOperationException;

    /**
     * $range : { name : { $gte : value, $lte : value } }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @throws InvalidParseOperationException
     */
    protected abstract void analyzeRange(String refCommand, JsonNode command, TypeRequest tr0, REQUEST req)
            throws InvalidParseOperationException;

    /**
     * $regex : { name : regex }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected abstract void analyzeRegex(String refCommand, JsonNode command, TypeRequest tr0, REQUEST req)
            throws InvalidParseOperationException;

    /**
     * $term : { name : term, name : term }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @throws InvalidParseOperationException
     */
    protected abstract void analyzeTerm(String refCommand, JsonNode command, TypeRequest tr0, REQUEST req)
            throws InvalidParseOperationException;

    /**
     * $wildcard : { name : term }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @throws InvalidParseOperationException
     */
    protected abstract void analyzeWildcard(String refCommand, JsonNode command, TypeRequest tr0, REQUEST req)
            throws InvalidParseOperationException;

    /**
     * $eq : { name : value }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @throws InvalidParseOperationException
     */
    protected abstract void analyzeEq(String refCommand, JsonNode command, TypeRequest tr0, REQUEST req)
            throws InvalidParseOperationException;

    /**
     * $exists : name
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected abstract void analyzeExistsMissing(String refCommand, JsonNode command, TypeRequest tr0, REQUEST req)
            throws InvalidParseOperationException;

    /**
     * $isNull : name
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected abstract void analyzeIsNull(String refCommand, JsonNode command, TypeRequest tr0, REQUEST req)
            throws InvalidParseOperationException;

    /**
     * $and : [ expression1, expression2, ... ]
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected abstract void analyzeAndNotNorOr(String refCommand, JsonNode command, TypeRequest tr0, REQUEST req)
            throws InvalidParseOperationException;

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("Request: ");
        for (final TypeRequest subrequest : getRequests()) {
            builder.append("\n");
            builder.append(subrequest.toString());
        }
        builder.append("\n\tLastLevel: " + lastDepth);
        builder.append("\n HintCache: " + hintCache);
        builder.append(" Limit: " + limit);
        builder.append(" Offset: " + offset);
        builder.append("\n\tOrderBy: " + orderBy);
        builder.append("\n\tProjection: " + projection);
        builder.append(" Usage: " + contractId);

        return builder.toString();
    }

    /**
     * @return the requests
     */
    public List<TypeRequest> getRequests() {
        return requests;
    }

    /**
     * @return the orderBy
     */
    public ObjectNode getOrderBy() {
        return orderBy;
    }

    /**
     * @return the sources
     */
    public List<String> getSources() {
        return sources;
    }

}
