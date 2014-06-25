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
	
    public EsQueryParser(boolean simul) {
        super(simul);
        usingElasticSearch = true;
    }
    
    /*
     * Here are 3 variations: 

* Query only: 

   { query: { text: {  _all: "foo bar" }}} 


* Filter only: 

   { query: { 
         constant_score: { 
             filter: {  term: { status: "open" }} 
         } 
   }} 

* Query and Filter: 

   { query: { 
         filtered: { 
             query:  {  text: { _all:   "foo bar"}} 
             filter: {  term: { status: "open" }} 
         } 
   }} 


So: 
--- 
1) You always need wrap your query in a top-level query element 
2) A "constant_score" query says "all docs are equal", so no scoring 
   has to happen - just the filter gets applied 
3) In the third example, filter reduces the number of docs that 
   can be matched (and scored) by the query 


There is also a top-level filter argument: 

{ 
  query:  { text: { _all: "foo bar" }}, 
  filter: { term: { status: "open" }} 
} 

For normal usage, you should NOT use this version.  It's purpose is 
different from the "filtered" query mentioned above. 

This is intended only to be used when you want to: 
 - run a query 
 - filter the results 
 - BUT show facets on the UNFILTERED results 

So this filter will be less efficient than the "filtered" query. 
     */
    /*
         In ElasticSearch : 
         Query => { "from" : offset, "size" : number, "sort" : [ SortFilter as "name" : { "order" : "asc|desc" } ], "query" : Query }
         FilteredQuery => { "filtered" : { "query" : { Query }, "filter" : { "limit" : { "value" : limit } } } }
      
     */
    

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
        sizeEs(tr0, element);
    }

    /**
     * @param tr0
     * @param element
     */
    protected final void sizeEs(TypeRequest tr0, Entry<String, JsonNode> element) {
        tr0.filterModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        tr0.filterModel[ELASTICSEARCH].putObject(ES_KEYWORDS.script.name()).put(ES_KEYWORDS.script.name(), 
                "doc['"+element.getKey()+"'].values.length == "+element.getValue());
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
        compareEs(tr0, req, element);
    }

    /**
     * @param tr0
     * @param req
     * @param element
     */
    protected final void compareEs(TypeRequest tr0, REQUEST req, Entry<String, JsonNode> element) {
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.range.name()).putObject(element.getKey()).set(req.name(), element.getValue());
    }

    /**
     * $flt : { $fields : [ name1, name2 ], $like : like_text }
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected final void analyzeXlt(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req) throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        tr0.isOnlyES = true;
        LOGGER.debug("ES only: {}", refCommand);
        ArrayNode fields = (ArrayNode) command.get(REQUESTARGS.fields.exactToken());
        JsonNode like = command.get(REQUESTARGS.like.exactToken());
        if (fields == null || like == null) {
            throw new InvalidParseOperationException("Incorrect command: "+refCommand+" : "+command);
        }
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        ObjectNode xlt = tr0.requestModel[ELASTICSEARCH].putObject(req.name());
        xlt.set(ES_KEYWORDS.fields.name(), fields);
        xlt.set(ES_KEYWORDS.like_text.name(), like);
    }

    /**
     * $search : { name : searchParameter }
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected final void analyzeSearch(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req) throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        tr0.isOnlyES = true;
        LOGGER.debug("ES only: {}", refCommand);
        Entry<String, JsonNode> element = JsonHandler.checkUnicity(refCommand, command);
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        ObjectNode objectES = tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.simple_query_string.name());
        objectES.set(ES_KEYWORDS.query.name(), element.getValue());
        objectES.putArray(ES_KEYWORDS.fields.name()).add(element.getKey());
    }

    /**
     * $match : { name : words, $max_expansions : n }
     * @param refCommand
     * @param command
     * @param tr0
     * @param req
     * @throws InvalidParseOperationException
     */
    protected final void analyzeMatch(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req) throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        tr0.isOnlyES = true;
        LOGGER.debug("ES only: {}", refCommand);
        JsonNode max = ((ObjectNode) command).remove(REQUESTARGS.max_expansions.exactToken());
        Entry<String, JsonNode> element = JsonHandler.checkUnicity(refCommand, command);
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        String attribute = element.getKey();
        if ((req == REQUEST.match_phrase_prefix || req == REQUEST.prefix) && isAttributeNotAnalyzed(attribute)) {
            tr0.requestModel[ELASTICSEARCH].putObject(REQUEST.prefix.name()).set(element.getKey(), element.getValue());
        } else {
            REQUEST req2 = req;
            if (req == REQUEST.prefix) {
                req2 = REQUEST.match_phrase_prefix;
            }
            if (max != null && ! max.isMissingNode()) {
                ObjectNode node = tr0.requestModel[ELASTICSEARCH].putObject(req2.name()).putObject(element.getKey());
                node.set(ES_KEYWORDS.query.name(), element.getValue());
                node.put(ES_KEYWORDS.max_expansions.name(), max.asInt());
            } else {
                tr0.requestModel[ELASTICSEARCH].putObject(req2.name()).set(element.getKey(), element.getValue());
            }
        }
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
        inEs(tr0, req, element);
    }

    /**
     * @param tr0
     * @param req
     * @param element
     */
    protected final void inEs(TypeRequest tr0, REQUEST req, Entry<String, JsonNode> element) {
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        ArrayNode objectES = null;
        if (req == REQUEST.nin) {
            objectES = tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.bool.name()).putObject(ES_KEYWORDS.must_not.name()).putObject(req.name()).putArray(element.getKey());
        } else {
            objectES = tr0.requestModel[ELASTICSEARCH].putObject(req.name()).putArray(element.getKey());
        }
        for (JsonNode value : element.getValue()) {
            objectES.add(value);
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
        rangeEs(tr0, req, element);
    }

    /**
     * @param tr0
     * @param req
     * @param element
     * @throws InvalidParseOperationException
     */
    protected final void rangeEs(TypeRequest tr0, REQUEST req, Entry<String, JsonNode> element)
            throws InvalidParseOperationException {
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        ObjectNode objectES = tr0.requestModel[ELASTICSEARCH].putObject(req.name()).putObject(element.getKey());
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
            objectES.set(arg.name(), requestItem.getValue());
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
        regexEs(tr0, entry);
    }

    /**
     * @param tr0
     * @param entry
     */
    protected final void regexEs(TypeRequest tr0, Entry<String, JsonNode> entry) {
        // possibility to use ES too with Query_String /regex/
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.regexp.name()).put(entry.getKey(), "/"+entry.getValue().asText()+"/");
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
        termEs(command, tr0);
    }

    /**
     * @param command
     * @param tr0
     */
    protected final void termEs(JsonNode command, TypeRequest tr0) {
        boolean multiple = false;
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        ArrayNode arrayES = null;
        if (command.size() > 1) {
            multiple = true;
            // ES
            arrayES = tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.bool.name()).putArray(ES_KEYWORDS.must.name());
        }
        for (Iterator<Entry<String, JsonNode>> iterator = command.fields(); iterator.hasNext();) {
            Entry<String, JsonNode> requestItem = iterator.next();
            String key = requestItem.getKey();
            JsonNode node = requestItem.getValue();
            String val = node.asText();
            if (! multiple) {
                tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.term.name()).put(key, val.toLowerCase());
                return;
            }
            arrayES.addObject().putObject(ES_KEYWORDS.term.name()).put(key, val.toLowerCase());
        }
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
        eqEs(tr0, req, entry);
    }

    /**
     * @param tr0
     * @param req
     * @param entry
     */
    protected final void eqEs(TypeRequest tr0, REQUEST req, Entry<String, JsonNode> entry) {
        tr0.requestModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        if (req == REQUEST.ne) {
            tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.bool.name()).putObject(ES_KEYWORDS.must_not.name())
                .putObject(ES_KEYWORDS.term.name()).set(entry.getKey(), entry.getValue());
        } else {
            tr0.requestModel[ELASTICSEARCH].putObject(ES_KEYWORDS.term.name()).set(entry.getKey(), entry.getValue());
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
        existsEs(command, tr0, req);
    }

    /**
     * @param command
     * @param tr0
     * @param req
     */
    protected final void existsEs(JsonNode command, TypeRequest tr0, REQUEST req) {
        // only fieldname
        String fieldname = command.asText();
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
    protected void analyzeIsNull(String refCommand, JsonNode command, TypeRequest tr0,
            REQUEST req) throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        isNullEs(command, tr0);
    }

    /**
     * @param command
     * @param tr0
     */
    protected final void isNullEs(JsonNode command, TypeRequest tr0) {
        // only fieldname
        String fieldname = command.asText();
        tr0.filterModel[ELASTICSEARCH] = JsonHandler.createObjectNode();
        ObjectNode objectES = tr0.filterModel[ELASTICSEARCH].putObject(ES_KEYWORDS.missing.name());
        objectES.put(ES_KEYWORDS.field.name(), fieldname);
        objectES.put(ES_KEYWORDS.existence.name(), false);
        objectES.put(ES_KEYWORDS.null_value.name(), true);
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
    protected final void booleanEs(String refCommand, JsonNode command, TypeRequest tr0, REQUEST req,
            List<TypeRequest> trlist) throws InvalidParseOperationException {
        boolean isFilter = false;
        boolean isRequest = false;
        if (command.isArray()) {
            // multiple elements in array
            for (JsonNode subcommand : command) {
                // one item
                Entry<String, JsonNode> requestItem = JsonHandler.checkUnicity(refCommand, subcommand);
                TypeRequest tr = analyzeOneCommand(requestItem.getKey(), requestItem.getValue());
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
            throw new InvalidParseOperationException("Boolean operator needs an array of expression: "+command);
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
            case nor: // ES does not support nor => or
            case or:
                if (isRequest) {
                    array = node.putArray(ES_KEYWORDS.should.name());
                }
                if (isFilter) {
                    arrayFilter = nodeFilter.putArray(ES_KEYWORDS.should.name());
                }
                break;
            case not:
                if (isRequest) {
                    array = node.putArray(ES_KEYWORDS.must_not.name());
                }
                if (isFilter) {
                    arrayFilter = nodeFilter.putArray(ES_KEYWORDS.must_not.name());
                }
                break;
            default:
                throw new InvalidParseOperationException("Not correctly parsed: "+refCommand);
        }
        for (int i = 0; i < trlist.size(); i++) {
            TypeRequest tr = trlist.get(i);
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
