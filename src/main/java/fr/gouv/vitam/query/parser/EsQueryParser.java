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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SimpleQueryStringFlag;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.exception.InvalidParseOperationException;
import fr.gouv.vitam.query.json.JsonHandler;
import fr.gouv.vitam.query.parser.ParserTokens.RANGEARGS;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;
import fr.gouv.vitam.query.parser.ParserTokens.REQUESTARGS;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * Version using ElasticSearch
 *
 * @author "Frederic Bregier"
 *
 */
public class EsQueryParser extends AbstractQueryParser {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(EsQueryParser.class);
    /**
     * ElasticSearch Keywords
     *
     */
    @SuppressWarnings("javadoc")
    public static enum ES_KEYWORDS {
        range, like_text, simple_query_string, query, fields, regexp, term, wildcard, 
        field, bool, must_not, should, must, missing, existence, null_value, script, max_expansions
    }


    /**
     * @param simul
     */
    public EsQueryParser(final boolean simul) {
        super(simul);
        usingElasticSearch = true;
    }

    /*
     * Here are 3 variations:
     * Query only:
     * { query: { text: { _all: "foo bar" }}}
     * Filter only:
     * { query: {
     * constant_score: {
     * filter: { term: { status: "open" }}
     * }
     * }}
     * Query and Filter:
     * { query: {
     * filtered: {
     * query: { text: { _all: "foo bar"}}
     * filter: { term: { status: "open" }}
     * }
     * }}
     * So:
     * ---
     * 1) You always need wrap your query in a top-level query element
     * 2) A "constant_score" query says "all docs are equal", so no scoring
     * has to happen - just the filter gets applied
     * 3) In the third example, filter reduces the number of docs that
     * can be matched (and scored) by the query
     * There is also a top-level filter argument:
     * {
     * query: { text: { _all: "foo bar" }},
     * filter: { term: { status: "open" }}
     * }
     * For normal usage, you should NOT use this version. It's purpose is
     * different from the "filtered" query mentioned above.
     * This is intended only to be used when you want to:
     * - run a query
     * - filter the results
     * - BUT show facets on the UNFILTERED results
     * So this filter will be less efficient than the "filtered" query.
     */
    /*
     * In ElasticSearch :
     * Query => { "from" : offset, "size" : number, "sort" : [ SortFilter as "name" : { "order" : "asc|desc" } ], "query" : Query
     * }
     * FilteredQuery => { "filtered" : { "query" : { Query }, "filter" : { "limit" : { "value" : limit } } } }
     */

    /**
     * $size : { name : length }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    @Override
    protected void analyzeSize(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        final Entry<String, JsonNode> element = JsonHandler.checkUnicity(refCommand, command);
        sizeEs(tr0, element);
    }

    /**
     * @param tr0
     * @param element
     */
    protected final void sizeEs(final TypeRequest tr0, final Entry<String, JsonNode> element) {
        tr0.filter = FilterBuilders.scriptFilter("doc['" + element.getKey() + "'].values.length == " + element.getValue());
    }

    /**
     * $gt : { name : value }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    @Override
    protected void analyzeCompare(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        final Entry<String, JsonNode> element = JsonHandler.checkUnicity(refCommand, command);
        compareEs(tr0, req, element);
    }

    /**
     * @param tr0
     * @param req
     * @param element
     * @throws InvalidParseOperationException 
     */
    protected final void compareEs(final TypeRequest tr0, final REQUEST req, final Entry<String, JsonNode> element) throws InvalidParseOperationException {
        String key = element.getKey();
        JsonNode node = element.getValue().findValue(ParserTokens.REQUESTARGS.date.exactToken());
        if (node == null) {
            node = element.getValue();
        } else {
            key += "." + ParserTokens.REQUESTARGS.date.exactToken();
        }
        switch (req) {
            case gt:
                tr0.query = QueryBuilders.rangeQuery(key).gt(getAsObject(node));
                break;
            case gte:
                tr0.query = QueryBuilders.rangeQuery(key).gte(getAsObject(node));
                break;
            case lt:
                tr0.query = QueryBuilders.rangeQuery(key).lt(getAsObject(node));
                break;
            case lte: 
                tr0.query = QueryBuilders.rangeQuery(key).lte(getAsObject(node));
                break;
            default:
                throw new InvalidParseOperationException("Not correctly parsed: " + req);
        }
    }

    /**
     * $flt : { $fields : [ name1, name2 ], $like : like_text }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    @Override
    protected final void analyzeXlt(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        tr0.isOnlyES = true;
        LOGGER.debug("ES only: {}", refCommand);
        final ArrayNode fields = (ArrayNode) command.get(REQUESTARGS.fields.exactToken());
        final JsonNode like = command.get(REQUESTARGS.like.exactToken());
        if (fields == null || like == null) {
            throw new InvalidParseOperationException("Incorrect command: " + refCommand + " : " + command);
        }
        String []names = new String[fields.size()];
        int i = 0;
        for (JsonNode name : fields) {
            names[i++] = name.toString();
        }
        switch (req) {
            case flt:
                tr0.query = QueryBuilders.fuzzyLikeThisQuery(names).likeText(like.toString());
                break;
            case mlt:
                tr0.query = QueryBuilders.moreLikeThisQuery(names).likeText(like.toString());
                break;
            default:
                throw new InvalidParseOperationException("Not correctly parsed: " + req);
        }
    }

    /**
     * $search : { name : searchParameter }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    @Override
    protected final void analyzeSearch(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        tr0.isOnlyES = true;
        LOGGER.debug("ES only: {}", refCommand);
        final Entry<String, JsonNode> element = JsonHandler.checkUnicity(refCommand, command);
        tr0.query = QueryBuilders.simpleQueryString(element.getValue().toString()).field(element.getKey());
    }

    /**
     * $match : { name : words, $max_expansions : n }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    @Override
    protected final void analyzeMatch(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        tr0.isOnlyES = true;
        LOGGER.debug("ES only: {}", refCommand);
        final JsonNode max = ((ObjectNode) command).remove(REQUESTARGS.max_expansions.exactToken());
        final Entry<String, JsonNode> element = JsonHandler.checkUnicity(refCommand, command);
        final String attribute = element.getKey();
        if ((req == REQUEST.match_phrase_prefix || req == REQUEST.prefix) && isAttributeNotAnalyzed(attribute)) {
            tr0.query = QueryBuilders.prefixQuery(element.getKey(), element.getValue().toString());
        } else {
            REQUEST req2 = req;
            if (req == REQUEST.prefix) {
                req2 = REQUEST.match_phrase_prefix;
            }
            if (max != null && !max.isMissingNode()) {
                switch (req2) {
                    case match:
                        tr0.query = QueryBuilders.matchQuery(element.getKey(), element.getValue().toString()).maxExpansions(max.asInt());
                        break;
                    case match_phrase:
                        tr0.query = QueryBuilders.matchPhraseQuery(element.getKey(), element.getValue().toString()).maxExpansions(max.asInt());
                        break;
                    case match_phrase_prefix:
                        tr0.query = QueryBuilders.matchPhrasePrefixQuery(element.getKey(), element.getValue().toString()).maxExpansions(max.asInt());
                        break;
                    default:
                        throw new InvalidParseOperationException("Not correctly parsed: " + req);
                }
            } else {
                switch (req) {
                    case match:
                        tr0.query = QueryBuilders.matchQuery(element.getKey(), element.getValue().toString());
                        break;
                    case match_phrase:
                        tr0.query = QueryBuilders.matchPhraseQuery(element.getKey(), element.getValue().toString());
                        break;
                    case match_phrase_prefix:
                        tr0.query = QueryBuilders.matchPhrasePrefixQuery(element.getKey(), element.getValue().toString());
                        break;
                    default:
                        throw new InvalidParseOperationException("Not correctly parsed: " + req);
                }
            }
        }
    }

    /**
     * $in : { name : [ value1, value2, ... ] }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @throws InvalidParseOperationException
     */
    @Override
    protected void analyzeIn(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        final Entry<String, JsonNode> element = JsonHandler.checkUnicity(refCommand, command);
        inEs(tr0, req, element);
    }

    /**
     * @param tr0
     * @param req
     * @param element
     */
    protected final void inEs(final TypeRequest tr0, final REQUEST req, final Entry<String, JsonNode> element) {
        String key = element.getKey();
        List<JsonNode> nodes = element.getValue().findValues(ParserTokens.REQUESTARGS.date.exactToken());
        if (nodes != null && ! nodes.isEmpty()) {
            key += "." + ParserTokens.REQUESTARGS.date.exactToken();
        }
        if (nodes != null && ! nodes.isEmpty()) {
            Set<Object> set = new HashSet<Object>();
            for (final JsonNode value : nodes) {
                set.add(getAsObject(value));
            }
            tr0.query = QueryBuilders.inQuery(key, set);
            if (req == REQUEST.nin) {
                tr0.query = QueryBuilders.boolQuery().mustNot(tr0.query);
            }
        } else {
            Set<Object> set = new HashSet<Object>();
            for (final JsonNode value : element.getValue()) {
                set.add(getAsObject(value));
            }
            tr0.query = QueryBuilders.inQuery(key, set);
            if (req == REQUEST.nin) {
                tr0.query = QueryBuilders.boolQuery().mustNot(tr0.query);
            }
        }
    }
    
    private static final Object getAsObject(JsonNode value) {
        if (value.isBoolean()) {
            return value.asBoolean();
        } else if (value.canConvertToLong()) {
            return value.asLong();
        } else if (value.isDouble()) {
            return value.asDouble();
        } else {
            return value.asText();
        }
    }

    /**
     * $range : { name : { $gte : value, $lte : value } }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @throws InvalidParseOperationException
     */
    @Override
    protected void analyzeRange(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        final Entry<String, JsonNode> element = JsonHandler.checkUnicity(refCommand, command);
        rangeEs(tr0, req, element);
    }

    /**
     * @param tr0
     * @param req
     * @param element
     * @throws InvalidParseOperationException
     */
    protected final void rangeEs(final TypeRequest tr0, final REQUEST req, final Entry<String, JsonNode> element)
            throws InvalidParseOperationException {
        String key = element.getKey();
        JsonNode node = element.getValue().findValue(ParserTokens.REQUESTARGS.date.exactToken());
        if (node != null) {
            key += "." + ParserTokens.REQUESTARGS.date.exactToken();
        }
        RangeQueryBuilder range = QueryBuilders.rangeQuery(key);
        for (final Iterator<Entry<String, JsonNode>> iterator = element.getValue().fields(); iterator.hasNext();) {
            final Entry<String, JsonNode> requestItem = iterator.next();
            RANGEARGS arg = null;
            try {
                final String skey = requestItem.getKey();
                if (skey.startsWith("$")) {
                    arg = RANGEARGS.valueOf(skey.substring(1));
                } else {
                    throw new InvalidParseOperationException("Invalid Range query command: " + requestItem);
                }
            } catch (final IllegalArgumentException e) {
                throw new InvalidParseOperationException("Invalid Range query command: " + requestItem, e);
            }
            node = requestItem.getValue().findValue(ParserTokens.REQUESTARGS.date.exactToken());
            if (node == null) {
                node = requestItem.getValue();
            }
            switch (arg) {
                case gt:
                    range.gt(getAsObject(node));
                    break;
                case gte:
                    range.gte(getAsObject(node));
                    break;
                case lt:
                    range.lt(getAsObject(node));
                    break;
                case lte: 
                    range.lte(getAsObject(node));
                    break;
                default:
                    throw new InvalidParseOperationException("Not correctly parsed: " + req);
            }
            tr0.query = range;
        }
    }

    /**
     * $regex : { name : regex }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    @Override
    protected void analyzeRegex(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        final Entry<String, JsonNode> entry = JsonHandler.checkUnicity(refCommand, command);
        regexEs(tr0, entry);
    }

    /**
     * @param tr0
     * @param entry
     */
    protected final void regexEs(final TypeRequest tr0, final Entry<String, JsonNode> entry) {
        // possibility to use ES too with Query_String /regex/
        tr0.query = QueryBuilders.regexpQuery(entry.getKey(), "/" + entry.getValue().asText() + "/");
    }

    /**
     * $term : { name : term, name : term }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @throws InvalidParseOperationException
     */
    @Override
    protected void analyzeTerm(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        termEs(command, tr0);
    }
    /**
     * @param command
     * @param tr0
     */
    protected final void termEs(final JsonNode command, final TypeRequest tr0) {
        boolean multiple = false;
        if (command.size() > 1) {
            multiple = true;
            // ES
            tr0.query = QueryBuilders.boolQuery();
        }
        for (final Iterator<Entry<String, JsonNode>> iterator = command.fields(); iterator.hasNext();) {
            final Entry<String, JsonNode> requestItem = iterator.next();
            String key = requestItem.getKey();
            JsonNode node = requestItem.getValue().findValue(ParserTokens.REQUESTARGS.date.exactToken());
            boolean isDate = false;
            if (node == null) {
                node = requestItem.getValue();
            } else {
                isDate = true;
                key += "." + ParserTokens.REQUESTARGS.date.exactToken();
            }
            if (node.isNumber()) {
                if (!multiple) {
                    tr0.query = QueryBuilders.termQuery(key, getAsObject(node));
                    return;
                }
                ((BoolQueryBuilder)tr0.query).must(QueryBuilders.termQuery(key, getAsObject(node)));
            } else {
                final String val = node.asText();
                QueryBuilder query = null;
                if (isAttributeNotAnalyzed(key) || isDate) {
                    query = QueryBuilders.termQuery(key, val);
                } else {
                    query = QueryBuilders.simpleQueryString("\""+val+"\"").field(key).flags(SimpleQueryStringFlag.PHRASE);
                    //tr0.query = QueryBuilders.matchPhrasePrefixQuery(key, val).maxExpansions(0);
                }
                if (!multiple) {
                    tr0.query = query;
                    return;
                }
                ((BoolQueryBuilder)tr0.query).must(query);
            }
        }
    }

    /**
     * $wildcard : { name : term }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @throws InvalidParseOperationException
     */
    @Override
    protected void analyzeWildcard(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        final Entry<String, JsonNode> entry = JsonHandler.checkUnicity(refCommand, command);
        wildcardEs(entry, tr0);
    }

    /**
     * @param entry
     * @param tr0
     */
    protected final void wildcardEs(final Entry<String, JsonNode> entry, final TypeRequest tr0) {
        final String key = entry.getKey();
        final JsonNode node = entry.getValue();
        final String val = node.asText();
        tr0.query = QueryBuilders.wildcardQuery(key, val);
    }

    /**
     * $eq : { name : value }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @throws InvalidParseOperationException
     */
    @Override
    protected void analyzeEq(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        final Entry<String, JsonNode> entry = JsonHandler.checkUnicity(refCommand, command);
        eqEs(tr0, req, entry);
    }

    /**
     * @param tr0
     * @param req
     * @param entry
     */
    protected final void eqEs(final TypeRequest tr0, final REQUEST req, final Entry<String, JsonNode> entry) {
        String key = entry.getKey();
        JsonNode node = entry.getValue().findValue(ParserTokens.REQUESTARGS.date.exactToken());
        boolean isDate = false;
        if (node == null) {
            node = entry.getValue();
        } else {
            isDate = true;
            key += "." + ParserTokens.REQUESTARGS.date.exactToken();
        }
        if (isAttributeNotAnalyzed(key) || isDate) {
            tr0.query = QueryBuilders.termQuery(key, getAsObject(node));
            if (req == REQUEST.ne) {
                tr0.query = QueryBuilders.boolQuery().mustNot(tr0.query);
            }
        } else {
            tr0.query = QueryBuilders.simpleQueryString("\""+getAsObject(node)+"\"").field(key).flags(SimpleQueryStringFlag.PHRASE);
            //tr0.query = QueryBuilders.matchPhrasePrefixQuery(key, getAsObject(node)).maxExpansions(0);
            if (req == REQUEST.ne) {
                tr0.query = QueryBuilders.boolQuery().mustNot(tr0.query);
            }
        }
    }

    /**
     * $exists : name
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    @Override
    protected void analyzeExistsMissing(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        existsEs(command, tr0, req);
    }

    /**
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException 
     */
    protected final void existsEs(final JsonNode command, final TypeRequest tr0, final REQUEST req) throws InvalidParseOperationException {
        // only fieldname
        final String fieldname = command.asText();
        switch (req) {
            case exists:
                tr0.filter = FilterBuilders.existsFilter(fieldname);
                break;
            case missing:
                tr0.filter = FilterBuilders.missingFilter(fieldname);
                break;
            default:
                throw new InvalidParseOperationException("Not correctly parsed: " + req);
        }
    }

    /**
     * $isNull : name
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    @Override
    protected void analyzeIsNull(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        isNullEs(command, tr0);
    }

    /**
     * @param command
     * @param tr0
     */
    protected final void isNullEs(final JsonNode command, final TypeRequest tr0) {
        // only fieldname
        final String fieldname = command.asText();
        tr0.filter = FilterBuilders.missingFilter(fieldname).existence(false).nullValue(true);
    }

    /**
     * $and : [ expression1, expression2, ... ]
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    @Override
    protected void analyzeAndNotNorOr(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        final List<TypeRequest> trlist = new ArrayList<>();
        booleanEs(refCommand, command, tr0, req, trlist);
    }

    /**
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @param trlist
     * @throws InvalidParseOperationException
     */
    protected final void booleanEs(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req,
            final List<TypeRequest> trlist) throws InvalidParseOperationException {
        boolean isFilter = false;
        boolean isRequest = false;
        if (command.isArray()) {
            // multiple elements in array
            for (final JsonNode subcommand : command) {
                // one item
                final Entry<String, JsonNode> requestItem = JsonHandler.checkUnicity(refCommand, subcommand);
                final TypeRequest tr = analyzeOneCommand(requestItem.getKey(), requestItem.getValue());
                trlist.add(tr);
                unionTransaction(tr0, tr);
                if (tr.filter != null) {
                    isFilter = true;
                }
                if (tr.query != null) {
                    isRequest = true;
                }
            }
        } else {
            throw new InvalidParseOperationException("Boolean operator needs an array of expression: " + command);
        }
        // ES
        if (isRequest) {
            tr0.query = QueryBuilders.boolQuery();
        }
        if (isFilter) {
            tr0.filter = FilterBuilders.boolFilter();
        }
        for (int i = 0; i < trlist.size(); i++) {
            final TypeRequest tr = trlist.get(i);
            if (isRequest && tr.query != null) {
                switch (req) {
                    case and:
                        ((BoolQueryBuilder) tr0.query).must(tr.query);
                        break;
                    case or:
                        ((BoolQueryBuilder) tr0.query).should(tr.query);
                        break;
                    case nor:
                    case not:
                        ((BoolQueryBuilder) tr0.query).mustNot(tr.query);
                        break;
                    default:
                        throw new InvalidParseOperationException("Not correctly parsed: " + req);
                }
            }
            if (isFilter && tr.filter != null) {
                switch (req) {
                    case and:
                        ((BoolFilterBuilder) tr0.filter).must(tr.filter);
                        break;
                    case or:
                        ((BoolFilterBuilder) tr0.filter).should(tr.filter);
                        break;
                    case nor:
                    case not:
                        ((BoolFilterBuilder) tr0.filter).mustNot(tr.filter);
                        break;
                    default:
                        throw new InvalidParseOperationException("Not correctly parsed: " + req);
                }
            }
        }
    }
}
