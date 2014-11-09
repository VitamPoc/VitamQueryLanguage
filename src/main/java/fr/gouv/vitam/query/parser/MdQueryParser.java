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

import org.bson.BSON;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.parser.ParserTokens.RANGEARGS;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;
import fr.gouv.vitam.utils.exception.InvalidParseOperationException;
import fr.gouv.vitam.utils.json.JsonHandler;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * Version using MongoDB
 *
 * @author "Frederic Bregier"
 *
 */
public class MdQueryParser extends AbstractQueryParser {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(MdQueryParser.class);

    /**
     * @param simul
     */
    public MdQueryParser(final boolean simul) {
        super(simul);
        usingMongoDb = true;
    }

    /*
     * In MongoDB : find(Query, Projection).sort(SortFilter).skip(SkipFilter).limit(LimitFilter);
     * In addition, one shall limit the scan by: find(Query, Projection)._addSpecial( "$maxscan", highlimit
     * ).sort(SortFilter).skip(SkipFilter).limit(LimitFilter);
     */

    @Override
    protected void checkRootTypeRequest(final TypeRequest tr, final JsonNode command, final int prevDepth)
            throws InvalidParseOperationException {
        if (tr.relativedepth > 1 || lastDepth - prevDepth > 1) {
            // MongoDB not allowed
            LOGGER.debug("ES only: {}", command);
            throw new InvalidParseOperationException("Command not allowed with MongoDB while Depth step: "
                    + (lastDepth - prevDepth) + ":" + lastDepth + ":" + prevDepth + ":" + tr.relativedepth + " " + tr);
        }
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
    @Override
    protected void analyzeSize(final String refCommand, final JsonNode command, final TypeRequest tr0, final REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: " + refCommand);
        }
        final Entry<String, JsonNode> element = JsonHandler.checkUnicity(refCommand, command);
        tr0.requestModel = JsonHandler.createObjectNode();
        tr0.requestModel.putObject(element.getKey()).set(refCommand, element.getValue());
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
        tr0.requestModel = JsonHandler.createObjectNode();
        tr0.requestModel.putObject(element.getKey()).set(refCommand, element.getValue());
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
        throw new InvalidParseOperationException("Command not allowed with MongoDB: " + refCommand);
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
        throw new InvalidParseOperationException("Command not allowed with MongoDB: " + refCommand);
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
        throw new InvalidParseOperationException("Command not allowed with MongoDB: " + refCommand);
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
        tr0.requestModel = JsonHandler.createObjectNode();
        final ArrayNode objectMD = tr0.requestModel.putObject(element.getKey()).putArray(refCommand);
        for (final JsonNode value : element.getValue()) {
            objectMD.add(value);
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
        tr0.requestModel = JsonHandler.createObjectNode();
        final ObjectNode objectMD = tr0.requestModel.putObject(element.getKey());
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
            objectMD.set(arg.exactToken(), requestItem.getValue());
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
        tr0.requestModel = JsonHandler.createObjectNode();
        tr0.requestModel.putObject(entry.getKey()).set(refCommand, entry.getValue());
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
        tr0.requestModel = JsonHandler.createObjectNode();
        tr0.requestModel.setAll((ObjectNode) command);
    }

    /**
     * $wildcard : { name : regex }
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
        tr0.requestModel = JsonHandler.createObjectNode();
        String value = entry.getValue().asText();
        value = value.replace('?', '.').replace("*", ".*");
        tr0.requestModel.putObject(entry.getKey()).put(REQUEST.regex.exactToken(), value);
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
        tr0.requestModel = JsonHandler.createObjectNode();
        if (req == REQUEST.ne) {
            tr0.requestModel.putObject(entry.getKey()).set(refCommand, entry.getValue());
        } else {
            tr0.requestModel.set(entry.getKey(), entry.getValue());
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
        // only fieldname
        final String fieldname = command.asText();
        tr0.requestModel = JsonHandler.createObjectNode();
        tr0.requestModel.putObject(fieldname).put(REQUEST.exists.exactToken(), req == REQUEST.exists);
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
        tr0.requestModel = JsonHandler.createObjectNode();
        tr0.requestModel.putObject(fieldname).put("$type", BSON.NULL);
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
        // MD
        tr0.requestModel = JsonHandler.createObjectNode();
        ArrayNode array = null;
        if (req == REQUEST.not) {
            if (trlist.size() == 1) {
                tr0.requestModel.set(REQUEST.not.exactToken(), trlist.get(0).requestModel);
            } else {
                array = tr0.requestModel.putObject(REQUEST.not.exactToken()).putArray(REQUEST.and.exactToken());
            }
        } else {
            array = tr0.requestModel.putArray(refCommand);
        }
        if (array != null) {
            for (int i = 0; i < trlist.size(); i++) {
                final TypeRequest tr = trlist.get(i);
                if (tr.requestModel != null) {
                    array.add(tr.requestModel);
                }
            }
        }
    }
}
