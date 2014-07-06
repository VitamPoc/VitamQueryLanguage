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
import fr.gouv.vitam.query.parser.ParserTokens.ACTIONFILTER;
import fr.gouv.vitam.query.parser.ParserTokens.GLOBAL;

/**
 * @author "Frederic Bregier"
 *
 */
public class Insert {
    protected ArrayNode requests;
    protected ObjectNode filter;
    protected ObjectNode data;

    /**
     *
     * @return this Insert
     */
    public final Insert resetFilter() {
        if (filter != null) {
            filter.removeAll();
        }
        return this;
    }

    /**
     *
     * @return this Insert
     */
    public final Insert resetData() {
        if (data != null) {
            data.removeAll();
        }
        return this;
    }

    /**
     *
     * @return this Insert
     */
    public final Insert resetRequests() {
        if (requests != null) {
            requests.removeAll();
        }
        return this;
    }

    /**
     * @param mult
     *            True to act on multiple elements, False to act only on 1 element
     * @return this Insert
     */
    public final Insert setMult(final boolean mult) {
        if (filter == null) {
            filter = JsonHandler.createObjectNode();
        }
        filter.put(ACTIONFILTER.mult.exactToken(), mult);
        return this;
    }

    /**
     * Note that if previous attributes have the same name, they will be replaced.
     *
     * @param data
     * @return this Insert
     */
    public final Insert addData(final ObjectNode... data) {
        if (this.data == null) {
            this.data = JsonHandler.createObjectNode();
        }
        for (final ObjectNode act : data) {
            if (!act.isMissingNode()) {
                this.data.setAll(act);
            }
        }
        return this;
    }

    /**
     *
     * @param requests
     * @return this Insert
     * @throws InvalidCreateOperationException
     */
    public final Insert addRequests(final Request... requests) throws InvalidCreateOperationException {
        if (this.requests == null) {
            this.requests = JsonHandler.createArrayNode();
        }
        for (final Request request : requests) {
            if (!request.isReady()) {
                throw new InvalidCreateOperationException("Request is not ready to be added: " + request.getCurrentRequest());
            }
            this.requests.add(request.getCurrentRequest());
        }
        return this;
    }

    /**
     *
     * @return the Final Insert containing all 3 parts: requests array, filter and data
     */
    public final ObjectNode getFinalInsert() {
        final ObjectNode node = JsonHandler.createObjectNode();
        if (requests != null && requests.size() > 0) {
            node.set(GLOBAL.query.exactToken(), requests);
        }
        if (filter != null && filter.size() > 0) {
            node.set(GLOBAL.filter.exactToken(), filter);
        }
        if (data != null && data.size() > 0) {
            node.set(GLOBAL.data.exactToken(), data);
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
     * @return the data
     */
    public final ObjectNode getData() {
        if (data == null) {
            return JsonHandler.createObjectNode();
        }
        return data;
    }
}
