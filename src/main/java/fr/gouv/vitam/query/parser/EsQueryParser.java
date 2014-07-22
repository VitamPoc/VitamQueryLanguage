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
        tr0.filterModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        tr0.filterModel[ELASTICSEARCH].putObject(ES_KEYWORDS.script.name()).put(ES_KEYWORDS.script.name(),
                "doc['" + element.getKey() + "'].values.length == " + element.getValue());
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
     */
    protected final void compareEs(final TypeRequest tr0, final REQUEST req, final Entry<String, JsonNode> element) {
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        String key = element.getKey();
        JsonNode node = element.getValue().findValue(ParserTokens.REQUESTARGS.date.exactToken());
        if (node == null) {
            node = element.getValue();
        } else {
            key += "." + ParserTokens.REQUESTARGS.date.exactToken();
        }
        tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.range.name()).putObject(key)
            .set(req.name(), node);
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
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        final ObjectNode xlt = tr0.requestModel[ELASTICSEARCH].putObject(req.name());
        xlt.set(ES_KEYWORDS.fields.name(), fields);
        xlt.set(ES_KEYWORDS.like_text.name(), like);
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
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        final ObjectNode objectES = tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.simple_query_string.name());
        objectES.set(ES_KEYWORDS.query.name(), element.getValue());
        objectES.putArray(ES_KEYWORDS.fields.name()).add(element.getKey());
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
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        final String attribute = element.getKey();
        if ((req == REQUEST.match_phrase_prefix || req == REQUEST.prefix) && isAttributeNotAnalyzed(attribute)) {
            tr0.requestModel[ELASTICSEARCH].putObject(REQUEST.prefix.name()).set(element.getKey(), element.getValue());
        } else {
            REQUEST req2 = req;
            if (req == REQUEST.prefix) {
                req2 = REQUEST.match_phrase_prefix;
            }
            if (max != null && !max.isMissingNode()) {
                final ObjectNode node = tr0.requestModel[ELASTICSEARCH].putObject(req2.name()).putObject(element.getKey());
                node.set(ES_KEYWORDS.query.name(), element.getValue());
                node.put(ES_KEYWORDS.max_expansions.name(), max.asInt());
            } else {
                tr0.requestModel[ELASTICSEARCH].putObject(req2.name()).set(element.getKey(), element.getValue());
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
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        ArrayNode objectES = null;
        String key = element.getKey();
        List<JsonNode> nodes = element.getValue().findValues(ParserTokens.REQUESTARGS.date.exactToken());
        if (nodes != null && ! nodes.isEmpty()) {
            key += "." + ParserTokens.REQUESTARGS.date.exactToken();
        }
        if (req == REQUEST.nin) {
            objectES = tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.bool.name()).putObject(ES_KEYWORDS.must_not.name())
                    .putObject(req.name()).putArray(key);
        } else {
            objectES = tr0.requestModel[ELASTICSEARCH].putObject(req.name()).putArray(key);
        }
        if (nodes != null && ! nodes.isEmpty()) {
            for (final JsonNode value : nodes) {
                objectES.add(value);
            }
        } else {
            for (final JsonNode value : element.getValue()) {
                objectES.add(value);
            }
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
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        String key = element.getKey();
        JsonNode node = element.getValue().findValue(ParserTokens.REQUESTARGS.date.exactToken());
        if (node != null) {
            key += "." + ParserTokens.REQUESTARGS.date.exactToken();
        }
        final ObjectNode objectES = tr0.requestModel[ELASTICSEARCH].putObject(req.name()).putObject(key);
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
            objectES.set(arg.name(), node);
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
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.regexp.name()).put(entry.getKey(),
                "/" + entry.getValue().asText() + "/");
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
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        ArrayNode arrayES = null;
        if (command.size() > 1) {
            multiple = true;
            // ES
            arrayES = tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.bool.name()).putArray(ES_KEYWORDS.must.name());
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
                    tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.term.name()).set(key, node);
                    return;
                }
                arrayES.addObject().putObject(ES_KEYWORDS.term.name()).set(key, node);
            } else {
                final String val = node.asText();
                if (!multiple) {
                    if (isAttributeNotAnalyzed(key) || isDate) {
                        tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.term.name()).put(key, val);
                        // XXX FIXME tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.term.name()).put(key, val.toLowerCase());
                    } else {
                        tr0.requestModel[ELASTICSEARCH].putObject(REQUEST.match_phrase_prefix.name()).put(key, val);
                    }
                    return;
                }
                if (isAttributeNotAnalyzed(key) || isDate) {
                    arrayES.addObject().putObject(ES_KEYWORDS.term.name()).put(key, val);
                    // XXX FIXME arrayES.addObject().putObject(ES_KEYWORDS.term.name()).put(key, val.toLowerCase());
                } else {
                    arrayES.addObject().putObject(REQUEST.match_phrase_prefix.name()).put(key, val);
                }
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
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        final String key = entry.getKey();
        final JsonNode node = entry.getValue();
        final String val = node.asText();
        tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.wildcard.name()).put(key, val);
        // XXX FIXME tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.wildcard.name()).put(key, val.toLowerCase());
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
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        String key = entry.getKey();
        JsonNode node = entry.getValue().findValue(ParserTokens.REQUESTARGS.date.exactToken());
        if (node == null) {
            node = entry.getValue();
        } else {
            key += "." + ParserTokens.REQUESTARGS.date.exactToken();
        }
        if (req == REQUEST.ne) {
            tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.bool.name()).putObject(ES_KEYWORDS.must_not.name())
                .putObject(ES_KEYWORDS.term.name()).set(key, node);
        } else {
            tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.term.name()).set(key, node);
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
     */
    protected final void existsEs(final JsonNode command, final TypeRequest tr0, final REQUEST req) {
        // only fieldname
        final String fieldname = command.asText();
        tr0.filterModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        tr0.filterModel[ELASTICSEARCH].putObject(req.name()).put(ES_KEYWORDS.field.name(), fieldname);
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
        tr0.filterModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        final ObjectNode objectES = tr0.filterModel[ELASTICSEARCH].putObject(ES_KEYWORDS.missing.name());
        objectES.put(ES_KEYWORDS.field.name(), fieldname);
        objectES.put(ES_KEYWORDS.existence.name(), false);
        objectES.put(ES_KEYWORDS.null_value.name(), true);
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
                if (tr.filterModel[ELASTICSEARCH] != null) {
                    isFilter = true;
                }
                if (tr.requestModel[ELASTICSEARCH] != null) {
                    isRequest = true;
                }
            }
        } else {
            throw new InvalidParseOperationException("Boolean operator needs an array of expression: " + command);
        }
        // ES
        ObjectNode node = null;
        ArrayNode array = null;
        if (isRequest) {
            tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
            node = tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.bool.name());
        }
        ObjectNode nodeFilter = null;
        ArrayNode arrayFilter = null;
        if (isFilter) {
            tr0.filterModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
            nodeFilter = tr0.filterModel[ELASTICSEARCH].putObject(ES_KEYWORDS.bool.name());
        }
        switch (req) {
            case and:
                if (isRequest) {
                    array = node.putArray(ES_KEYWORDS.must.name());
                }
                if (isFilter) {
                    arrayFilter = nodeFilter.putArray(ES_KEYWORDS.must.name());
                }
                break;
            case or:
                if (isRequest) {
                    array = node.putArray(ES_KEYWORDS.should.name());
                }
                if (isFilter) {
                    arrayFilter = nodeFilter.putArray(ES_KEYWORDS.should.name());
                }
                break;
            case nor: // ES does not support nor => not
            case not:
                if (isRequest) {
                    array = node.putArray(ES_KEYWORDS.must_not.name());
                }
                if (isFilter) {
                    arrayFilter = nodeFilter.putArray(ES_KEYWORDS.must_not.name());
                }
                break;
            default:
                throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        for (int i = 0; i < trlist.size(); i++) {
            final TypeRequest tr = trlist.get(i);
            if (isRequest) {
                if (tr.requestModel[ELASTICSEARCH] != null) {
                    array.add(tr.requestModel[ELASTICSEARCH]);
                }
            }
            if (isFilter) {
                if (tr.filterModel[ELASTICSEARCH] != null) {
                    arrayFilter.add(tr.filterModel[ELASTICSEARCH]);
                }
            }
        }
    }
}
