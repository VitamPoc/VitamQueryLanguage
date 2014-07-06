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

import fr.gouv.vitam.query.exception.InvalidParseOperationException;
import fr.gouv.vitam.query.json.JsonHandler;
import fr.gouv.vitam.query.parser.ParserTokens.RANGEARGS;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * Version using CouchBase
 *
 * refId will be used to store 0: WHERE part
 *
 * @author "Frederic Bregier"
 *
 */
public class CbQueryParser extends AbstractQueryParser {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(CbQueryParser.class);

    /**
     * @param simul
     */
    public CbQueryParser(final boolean simul) {
        super(simul);
        usingCouchBase = true;
    }

    /*
     * In CouchBase : select DISTINCT Projection from xxx over yyy where Query and Filter order by SortFilter limit LimitFilter
     * offset skipFilter
     */

    @Override
    protected void checkRootTypeRequest(final TypeRequest tr, final JsonNode command, final int prevDepth)
            throws InvalidParseOperationException {
        if (tr.depth > 1 || lastDepth - prevDepth > 1) {
            // CouchBase not allowed
            LOGGER.debug("ES only: {}", command);
            throw new InvalidParseOperationException("Command not allowed with CouchBase while Depth step: "
                    + (lastDepth - prevDepth) + ":" + lastDepth + ":" + prevDepth + ":" + tr.depth + " " + tr);
        }
    }

    /**
     * $size : { name : length }
     *
     * => LENGTH(name) = length
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
        tr0.requestCb = " LENGTH(" + element.getKey() + ") = " + element.getValue();
    }

    /**
     * $gt : { name : value }
     *
     * => name > value
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
     * $flt : { $fields : [ name1, name2 ], $like : like_text }
     *
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    @Override
    protected void analyzeXlt(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        throw new InvalidParseOperationException("Command not allowed with CouchBase: " + refCommand);
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
    protected void analyzeSearch(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        throw new InvalidParseOperationException("Command not allowed with CouchBase: " + refCommand);
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
    protected void analyzeMatch(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        throw new InvalidParseOperationException("Command not allowed with CouchBase: " + refCommand);
    }

    /**
     * $in : { name : [ value1, value2, ... ] }
     *
     * => WHERE name IN [value1 , value2, ... ]
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
     * => name >= value AND name <= value
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
     * => name LIKE regex ???
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
        final String val = entry.getValue().asText().replace(".*", "%").replace('.', '_').replace('?', '_').replace("^", "");
        tr0.requestCb = " " + entry.getKey() + " LIKE \"" + val + "\"";
    }

    /**
     * $term : { name : term, name : term }
     *
     * => name = term AND name = term
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
        if (command.isArray()) {
            // multiple elements in array
            for (final JsonNode subcommand : command) {
                // one item
                final Entry<String, JsonNode> requestItem = JsonHandler.checkUnicity(refCommand, subcommand);
                final TypeRequest tr = analyzeOneCommand(requestItem.getKey(), requestItem.getValue());
                trlist.add(tr);
                unionTransaction(tr0, tr);
            }
        } else {
            throw new InvalidParseOperationException("Boolean operator needs an array of expression: " + command);
        }
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
