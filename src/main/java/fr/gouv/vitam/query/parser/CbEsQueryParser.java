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

import fr.gouv.vitam.query.parser.ParserTokens.RANGEARGS;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;
import fr.gouv.vitam.utils.exception.InvalidParseOperationException;
import fr.gouv.vitam.utils.json.JsonHandler;

/**
 * Version using CouchBase and ElasticSearch
 *
 * @author "Frederic Bregier"
 *
 */
public class CbEsQueryParser extends EsQueryParser {
    /**
     * @param simul
     */
    public CbEsQueryParser(final boolean simul) {
        super(simul);
        usingCouchBase = true;
    }

    /*
     * In MongoDB : find(Query, Projection).sort(SortFilter).skip(SkipFilter).limit(LimitFilter);
     * In addition, one shall limit the scan by: find(Query, Projection)._addSpecial( "$maxscan", highlimit
     * ).sort(SortFilter).skip(SkipFilter).limit(LimitFilter);
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
        tr0.requestCb = " LENGTH(" + element.getKey() + ") = " + element.getValue();
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
        String op = null;
        switch (req) {
            case gt:
                op = " > ";
                break;
            case gte:
                op = " >= ";
                break;
            case lt:
                op = " < ";
                break;
            case lte:
                op = " <= ";
                break;
            default:
                break;

        }
        tr0.requestCb = " " + element.getKey() + op + element.getValue();
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
        final StringBuilder bval = new StringBuilder(" [ ");
        boolean notFirst = false;
        for (final JsonNode value : element.getValue()) {
            if (notFirst) {
                bval.append(", ");
            } else {
                notFirst = true;
            }
            bval.append(value);
        }
        bval.append("] ");
        tr0.requestCb = " " + element.getKey() + (req == REQUEST.nin ? " NOT IN " : " IN ") + bval.toString();
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
        String val = null;
        for (final Iterator<Entry<String, JsonNode>> iterator = element.getValue().fields(); iterator.hasNext();) {
            final Entry<String, JsonNode> requestItem = iterator.next();
            RANGEARGS arg = null;
            try {
                final String key = requestItem.getKey();
                if (key.startsWith("$")) {
                    arg = RANGEARGS.valueOf(requestItem.getKey().substring(1));
                } else {
                    throw new InvalidParseOperationException("Invalid Range query command: " + requestItem);
                }
            } catch (final IllegalArgumentException e) {
                throw new InvalidParseOperationException("Invalid Range query command: " + requestItem, e);
            }
            String op = null;
            switch (arg) {
                case gt:
                    op = " > ";
                    break;
                case gte:
                    op = " >= ";
                    break;
                case lt:
                    op = " < ";
                    break;
                case lte:
                    op = " <= ";
                    break;
                default:
                    break;

            }
            if (val != null) {
                val += " AND " + element.getKey() + op + requestItem.getValue();
            } else {
                val = " " + element.getKey() + op + requestItem.getValue();
            }
        }
        tr0.requestCb = val;
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
        final String val = entry.getValue().asText().replace(".*", "%").replace('.', '_').replace('?', '_').replace("^", "");
        tr0.requestCb = " " + entry.getKey() + " LIKE \"" + val + "\"";
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
        String val = null;
        for (final Iterator<Entry<String, JsonNode>> iterator = command.fields(); iterator.hasNext();) {
            final Entry<String, JsonNode> requestItem = iterator.next();
            if (val != null) {
                val += " AND " + requestItem.getKey() + " == " + requestItem.getValue();
            } else {
                val = " " + requestItem.getKey() + " == " + requestItem.getValue();
            }
        }
        tr0.requestCb = val;
    }

    /**
     * $wildcard : { name : term }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
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
        final String val = entry.getValue().asText().replace(".*", "%").replace('.', '_').replace('?', '_').replace("^", "");
        tr0.requestCb = " " + entry.getKey() + " LIKE \"" + val + "\"";
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
        String op = null;
        switch (req) {
            case eq:
                op = " = ";
                break;
            case ne:
                op = " != ";
                break;
            default:
                break;

        }
        tr0.requestCb = " " + entry.getKey() + op + entry.getValue();
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
        // only fieldname
        final String fieldname = command.asText();
        String op = null;
        switch (req) {
            case exists:
                op = " IS NOT MISSING ";
                break;
            case missing:
                op = " IS MISSING ";
                break;
            case isNull:
                op = " IS NULL ";
                break;
            default:
                break;

        }
        tr0.requestCb = " " + fieldname + op;
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
        // only fieldname
        final String fieldname = command.asText();
        tr0.requestCb = " " + fieldname + " IS NULL ";
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
        String val = null;
        String op = null;
        switch (req) {
            case and:
                op = " AND ";
                break;
            case nor:
                // no NOR support
                op = " OR ";
                break;
            case not:
                op = " AND ";
                break;
            case or:
                op = " OR ";
                break;
            default:
                break;

        }
        for (int i = 0; i < trlist.size(); i++) {
            final TypeRequest tr = trlist.get(i);
            if (tr.requestCb != null) {
                if (val != null) {
                    val += op;
                } else {
                    val = " ";
                }
                val += tr.requestCb;
            }
        }
        if (req == REQUEST.not) {
            val = " NOT (" + val + ") ";
        }
        tr0.requestCb = val;
    }
}
