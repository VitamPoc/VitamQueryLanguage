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
package fr.gouv.vitam.query.construct;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.construct.request.Request;
import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.json.JsonHandler;
import fr.gouv.vitam.query.parser.ParserTokens.GLOBAL;
import fr.gouv.vitam.query.parser.ParserTokens.PROJECTION;
import fr.gouv.vitam.query.parser.ParserTokens.REQUESTFILTER;

/**
 * @author "Frederic Bregier"
 *
 */
public class Query {
    protected ArrayNode requests;
    protected ObjectNode filter, projection;
    /**
     * 
     * @return this Query
     */
    public final Query resetHintFilter() {
        if (filter != null) {
            filter.remove(REQUESTFILTER.hint.exactToken());
        }
        return this;
    }
    /**
     * 
     * @return this Query
     */
    public final Query resetLimitFilter() {
        if (filter != null) {
            filter.remove(REQUESTFILTER.offset.exactToken());
            filter.remove(REQUESTFILTER.limit.exactToken());
        }
        return this;
    }
    /**
     * 
     * @return this Query
     */
    public final Query resetOrderByFilter() {
        if (filter != null) {
            filter.remove(REQUESTFILTER.orderby.exactToken());
        }
        return this;
    }
    /**
     * 
     * @return this Query
     */
    public final Query resetUsedProjection() {
        if (projection != null) {
            projection.remove(PROJECTION.fields.exactToken());
        }
        return this;
    }
    /**
     * 
     * @return this Query
     */
    public final Query resetUsageProjection() {
        if (projection != null) {
            projection.remove(PROJECTION.usage.exactToken());
        }
        return this;
    }
    /**
     * 
     * @return this Query
     */
    public final Query resetRequests() {
        if (requests != null) {
            requests.removeAll();
        }
        return this;
    }
    /**
     * @param offset ignored if 0
     * @param limit ignored if 0
     * @return this Query
     */
    public final Query setLimitFilter(long offset, long limit) {
        if (filter == null) {
            filter = JsonHandler.createObjectNode();
        }
        resetLimitFilter();
        if (offset > 0) {
            filter.put(REQUESTFILTER.offset.exactToken(), offset);
        }
        if (limit > 0) {
            filter.put(REQUESTFILTER.limit.exactToken(), limit);
        }
        return this;
    }
    /**
     * 
     * @param hints
     * @return this Query
     */
    public final Query addHintFilter(String ... hints) {
        if (filter == null) {
            filter = JsonHandler.createObjectNode();
        }
        ArrayNode array = (ArrayNode) filter.get(REQUESTFILTER.hint.exactToken());
        if (array == null || array.isMissingNode()) {
            array = filter.putArray(REQUESTFILTER.hint.exactToken());
        }
        for (String hint : hints) {
            if (hint == null || hint.trim().isEmpty()) {
                continue;
            }
            array.add(hint.trim());
        }
        return this;
    }
    /**
     * 
     * @param variableNames
     * @return this Query
     */
    public final Query addOrderByAscFilter(String ... variableNames) {
        if (filter == null) {
            filter = JsonHandler.createObjectNode();
        }
        ObjectNode node = (ObjectNode) filter.get(REQUESTFILTER.orderby.exactToken());
        if (node == null || node.isMissingNode()) {
            node = filter.putObject(REQUESTFILTER.orderby.exactToken());
        }
        for (String var : variableNames) {
            if (var == null || var.trim().isEmpty()) {
                continue;
            }
            node.put(var.trim(), 1);
        }
        return this;
    }
    /**
     * 
     * @param variableNames
     * @return this Query
     */
    public final Query addOrderByDescFilter(String ... variableNames) {
        if (filter == null) {
            filter = JsonHandler.createObjectNode();
        }
        ObjectNode node = (ObjectNode) filter.get(REQUESTFILTER.orderby.exactToken());
        if (node == null || node.isMissingNode()) {
            node = filter.putObject(REQUESTFILTER.orderby.exactToken());
        }
        for (String var : variableNames) {
            if (var == null || var.trim().isEmpty()) {
                continue;
            }
            node.put(var.trim(), -1);
        }
        return this;
    }

    /**
     * 
     * @param variableNames
     * @return this Query
     */
    public final Query addUsedProjection(String ... variableNames) {
        if (projection == null) {
            projection = JsonHandler.createObjectNode();
        }
        ObjectNode node = (ObjectNode) projection.get(PROJECTION.fields.exactToken());
        if (node == null || node.isMissingNode()) {
            node = projection.putObject(PROJECTION.fields.exactToken());
        }
        for (String var : variableNames) {
            if (var == null || var.trim().isEmpty()) {
                continue;
            }
            node.put(var.trim(), 1);
        }
        return this;
    }
    /**
     * 
     * @param variableNames
     * @return this Query
     */
    public final Query addUnusedProjection(String ... variableNames) {
        if (projection == null) {
            projection = JsonHandler.createObjectNode();
        }
        ObjectNode node = (ObjectNode) projection.get(PROJECTION.fields.exactToken());
        if (node == null || node.isMissingNode()) {
            node = projection.putObject(PROJECTION.fields.exactToken());
        }
        for (String var : variableNames) {
            if (var == null || var.trim().isEmpty()) {
                continue;
            }
            node.put(var.trim(), 0);
        }
        return this;
    }
    /**
     * 
     * @param usage
     * @return this Query
     */
    public final Query setUsageProjection(String usage) {
        if (projection == null) {
            projection = JsonHandler.createObjectNode();
        }
        if (usage == null || usage.trim().isEmpty()) {
            return this;
        }
        projection.put(PROJECTION.usage.exactToken(), usage.trim());
        return this;
    }

    /**
     * 
     * @param requests
     * @return The Query 
     * @throws InvalidCreateOperationException 
     */
    public final Query addRequests(Request ... requests) throws InvalidCreateOperationException {
        if (this.requests == null) {
            this.requests = JsonHandler.createArrayNode();
        }
        for (Request request : requests) {
            if (! request.isReady()) {
                throw new InvalidCreateOperationException("Request is not ready to be added: "+request.getCurrentRequest());
            }
            this.requests.add(request.getCurrentRequest());
        }
        return this;
    }
    /**
     * 
     * @return the Final Query containing all 3 parts: requests array, filter and projection
     */
    public final ObjectNode getFinalQuery() {
        ObjectNode node = JsonHandler.createObjectNode();
        if (requests != null && requests.size() > 0) {
            node.set(GLOBAL.query.exactToken(), requests);
        }
        if (filter != null && filter.size() > 0) {
            node.set(GLOBAL.filter.exactToken(), filter);
        }
        if (projection != null && projection.size() > 0) {
            node.set(GLOBAL.projection.exactToken(), projection);
        }
        return node;
    }
    /**
     * @return the requests array
     */
    public final ArrayNode getRequests() {
        if (requests == null) {
            return JsonHandler.createArrayNode();
        }
        return requests;
    }
    /**
     * @return the filter
     */
    public final ObjectNode getFilter() {
        if (filter == null) {
            return JsonHandler.createObjectNode();
        }
        return filter;
    }
    /**
     * @return the projection
     */
    public final ObjectNode getProjection() {
        if (projection == null) {
            return JsonHandler.createObjectNode();
        }
        return projection;
    }
}
