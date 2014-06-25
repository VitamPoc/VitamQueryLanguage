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

import fr.gouv.vitam.query.exception.InvalidParseOperationException;
import fr.gouv.vitam.query.json.JsonHandler;
import fr.gouv.vitam.query.parser.ParserTokens.RANGEARGS;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;
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
	
    public MdQueryParser(boolean simul) {
        super(simul);
        usingMongoDb = true;
    }
    
    /*
        In MongoDB : find(Query, Projection).sort(SortFilter).skip(SkipFilter).limit(LimitFilter);
        In addition, one shall limit the scan by: find(Query, Projection)._addSpecial( "$maxscan", highlimit ).sort(SortFilter).skip(SkipFilter).limit(LimitFilter);
     */
    
    protected void checkRootTypeRequest(TypeRequest tr, JsonNode command, int prevDepth)
            throws InvalidParseOperationException {
        if (tr.depth > 1 || lastDepth - prevDepth > 1) {
            // MongoDB not allowed
        	LOGGER.debug("ES only: {}", command);
            throw new InvalidParseOperationException("Command not allowed with MongoDB while Depth step: "+(lastDepth-prevDepth)+":"+lastDepth+":"+prevDepth+":"+tr.depth+" "+tr);
        }
    }

    /**
     * $size : { name : length }
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected void analyzeSize(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req) throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        Entry<String, JsonNode> element = JsonHandler.checkUnicity(refCommand, command);
        tr0.requestModel[MONGODB] = JsonHandler.createObjectNode();
        tr0.requestModel[MONGODB].putObject(element.getKey()).set(refCommand, element.getValue());
    }

    /**
     * $gt : { name : value }
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected void analyzeCompare(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req) throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        Entry<String, JsonNode> element = JsonHandler.checkUnicity(refCommand, command);
        tr0.requestModel[MONGODB] = JsonHandler.createObjectNode();
        tr0.requestModel[MONGODB].putObject(element.getKey()).set(refCommand, element.getValue());
    }

    /**
     * $flt : { $fields : [ name1, name2 ], $like : like_text }
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected void analyzeXlt(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req) throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        throw new InvalidParseOperationException("Command not allowed with MongoDB: "+refCommand);
    }

    /**
     * $search : { name : searchParameter }
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected void analyzeSearch(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req) throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        throw new InvalidParseOperationException("Command not allowed with MongoDB: "+refCommand);
    }

    /**
     * $match : { name : words, $max_expansions : n }
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected void analyzeMatch(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req) throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        throw new InvalidParseOperationException("Command not allowed with MongoDB: "+refCommand);
    }

    /**
     * $in : { name : [ value1, value2, ... ] }
     * @param refCommand
     * @param command
     * @param tr0
     * @throws InvalidParseOperationException
     */
    protected void analyzeIn(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        Entry<String, JsonNode> element = JsonHandler.checkUnicity(refCommand, command);
        tr0.requestModel[MONGODB] = JsonHandler.createObjectNode();
        ArrayNode objectMD = tr0.requestModel[MONGODB].putObject(element.getKey()).putArray(refCommand);
        for (JsonNode value : element.getValue()) {
            objectMD.add(value);
        }
    }

    /**
     * $range : { name : { $gte : value, $lte : value } }
     * @param refCommand
     * @param command
     * @param tr0
     * @throws InvalidParseOperationException
     */
    protected void analyzeRange(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        Entry<String, JsonNode> element = JsonHandler.checkUnicity(refCommand, command);
        tr0.requestModel[MONGODB] = JsonHandler.createObjectNode();
        ObjectNode objectMD = tr0.requestModel[MONGODB].putObject(element.getKey());
        for (Iterator<Entry<String, JsonNode>> iterator = element.getValue().fields(); iterator.hasNext();) {
            Entry<String, JsonNode> requestItem = iterator.next();
            RANGEARGS arg = null;
            try {
                String key = requestItem.getKey();
                if (key.startsWith("$")) {
                    arg = RANGEARGS.valueOf(requestItem.getKey().substring(1));
                } else {
                    throw new InvalidParseOperationException("Invalid Range query command: "+requestItem);
                }
            } catch (IllegalArgumentException e) {
                throw new InvalidParseOperationException("Invalid Range query command: "+requestItem, e);
            }
            objectMD.set(arg.exactToken(), requestItem.getValue());
        }
    }

    /**
     * $regex : { name : regex }
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected void analyzeRegex(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req) throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        Entry<String, JsonNode> entry = JsonHandler.checkUnicity(refCommand, command);
        tr0.requestModel[MONGODB] = JsonHandler.createObjectNode();
        tr0.requestModel[MONGODB].putObject(entry.getKey()).set(refCommand, entry.getValue());
    }

    /**
     * $term : { name : term, name : term }
     * 
     * @param refCommand
     * @param command
     * @param tr0
     * @throws InvalidParseOperationException
     */
    protected void analyzeTerm(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        tr0.requestModel[MONGODB] = JsonHandler.createObjectNode();
        tr0.requestModel[MONGODB].setAll((ObjectNode) command);
    }

    /**
     * $eq : { name : value }
     * 
     * @param refCommand
     * @param command
     * @param tr0
     * @throws InvalidParseOperationException
     */
    protected void analyzeEq(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req)
            throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        Entry<String, JsonNode> entry = JsonHandler.checkUnicity(refCommand, command);
        tr0.requestModel[MONGODB] = JsonHandler.createObjectNode();
        if (req == REQUEST.ne) {
            tr0.requestModel[MONGODB].putObject(entry.getKey()).set(refCommand, entry.getValue());
        } else {
            tr0.requestModel[MONGODB].set(entry.getKey(), entry.getValue());
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
    protected void analyzeExistsMissing(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req) throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        // only fieldname
        String fieldname = command.asText();
        tr0.requestModel[MONGODB] = JsonHandler.createObjectNode();
        tr0.requestModel[MONGODB].putObject(fieldname).put(REQUEST.exists.exactToken(), req == REQUEST.exists);
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
    protected void analyzeIsNull(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req) throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        // only fieldname
        String fieldname = command.asText();
        tr0.requestModel[MONGODB] = JsonHandler.createObjectNode();
        tr0.requestModel[MONGODB].putObject(fieldname).put("$type", BSON.NULL);
    }
    
    /**
     * $and : [ expression1, expression2, ... ]
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected void analyzeAndNotNorOr(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req) throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        List<TypeRequest> trlist = new ArrayList<>();
        if (command.isArray()) {
            // multiple elements in array
            for (JsonNode subcommand : command) {
                // one item
                Entry<String, JsonNode> requestItem = JsonHandler.checkUnicity(refCommand, subcommand);
                TypeRequest tr = analyzeOneCommand(requestItem.getKey(), requestItem.getValue());
                trlist.add(tr);
                unionTransaction(tr0, tr);
            }
        } else {
            throw new InvalidParseOperationException("Boolean operator needs an array of expression: "+command);
        }
        // MD
        tr0.requestModel[MONGODB] = JsonHandler.createObjectNode();
        ArrayNode array = null;
        if (req == REQUEST.not) {
            if (trlist.size() == 1) {
                tr0.requestModel[MONGODB].set(REQUEST.not.exactToken(), trlist.get(0).requestModel[MONGODB]);
            } else {
                array = tr0.requestModel[MONGODB].putObject(REQUEST.not.exactToken()).putArray(REQUEST.and.exactToken());
            }
        } else {
            array = tr0.requestModel[MONGODB].putArray(refCommand);
        }
        if (array != null) {
            for (int i = 0; i < trlist.size(); i++) {
                TypeRequest tr = trlist.get(i);
                if (tr.requestModel[MONGODB] != null) {
                    array.add(tr.requestModel[MONGODB]);
                }
            }
        }
    }
}
