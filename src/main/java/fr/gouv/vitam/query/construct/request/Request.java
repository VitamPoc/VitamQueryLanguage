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
package fr.gouv.vitam.query.construct.request;

import java.util.Date;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.json.JsonHandler;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;
import fr.gouv.vitam.query.parser.ParserTokens.REQUESTARGS;

/**
 * @author "Frederic Bregier"
 *
 */
public class Request {
    protected static final String DATE = "$date";
    protected ObjectNode currentRequest;
    protected JsonNode currentObject;
    protected REQUEST currentREQUEST;
    protected boolean ready;

    protected final void createRequestArray(final REQUEST request) {
        currentObject = ((ObjectNode) currentObject).putArray(request.exactToken());
    }

    protected final void createRequestVariable(final REQUEST request, final String variableName)
            throws InvalidCreateOperationException {
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request " + request + " cannot be created with empty variable name");
        }
        ((ObjectNode) currentObject).put(request.exactToken(), variableName.trim());
    }

    protected final void createRequestVariableValue(final REQUEST request, final String variableName, final long value)
            throws InvalidCreateOperationException {
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request " + request + " cannot be created with empty variable name");
        }
        currentObject = ((ObjectNode) currentObject).putObject(request.exactToken());
        ((ObjectNode) currentObject).put(variableName.trim(), value);
    }

    protected final void createRequestVariableValue(final REQUEST request, final String variableName, final double value)
            throws InvalidCreateOperationException {
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request " + request + " cannot be created with empty variable name");
        }
        currentObject = ((ObjectNode) currentObject).putObject(request.exactToken());
        ((ObjectNode) currentObject).put(variableName.trim(), value);
    }

    protected final void createRequestVariableValue(final REQUEST request, final String variableName, final String value)
            throws InvalidCreateOperationException {
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request " + request + " cannot be created with empty variable name");
        }
        currentObject = ((ObjectNode) currentObject).putObject(request.exactToken());
        ((ObjectNode) currentObject).put(variableName.trim(), value);
    }

    protected final void createRequestVariableValue(final REQUEST request, final String variableName, final Date value)
            throws InvalidCreateOperationException {
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request " + request + " cannot be created with empty variable name");
        }
        currentObject = ((ObjectNode) currentObject).putObject(request.exactToken());
        ((ObjectNode) currentObject).putObject(variableName.trim()).put(DATE, value.toString());
    }

    protected final void createRequestVariableValue(final REQUEST request, final String variableName, final boolean value)
            throws InvalidCreateOperationException {
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request " + request + " cannot be created with empty variable name");
        }
        currentObject = ((ObjectNode) currentObject).putObject(request.exactToken());
        ((ObjectNode) currentObject).put(variableName.trim(), value);
    }

    /**
     * Empty constructor
     */
    protected Request() {
        currentRequest = JsonHandler.createObjectNode();
        currentObject = currentRequest;
        currentREQUEST = null;
        ready = false;
    }

    /**
     * Clean the object
     */
    protected void clean() {
        cleanDepth();
    }

    /**
     * Removing depth and relativedepth
     */
    protected void cleanDepth() {
        currentRequest.remove(REQUESTARGS.depth.exactToken());
        currentRequest.remove(REQUESTARGS.relativedepth.exactToken());
    }

    /**
     *
     * @param depth
     *            1 to ignore
     * @return the single query ready to be added to global Query (remove previous depth and relativedepth if any)
     */
    public final Request setExactDepthLimit(final int depth) {
        cleanDepth();
        if (depth != 1) {
            currentRequest.put(REQUESTARGS.depth.exactToken(), depth);
        }
        return this;
    }

    /**
     *
     * @param relativedepth
     * @return the single query ready to be added to global Query (remove previous depth and relativedepth if any)
     */
    public final Request setRelativeDepthLimit(final int relativedepth) {
        cleanDepth();
        currentRequest.put(REQUESTARGS.relativedepth.exactToken(), relativedepth);
        return this;
    }

    /**
     * @return the currentRequest
     */
    public ObjectNode getCurrentRequest() {
        return currentRequest;
    }

    /**
     * @return the currentObject (internal use only during parse)
     */
    public JsonNode getCurrentObject() {
        return currentObject;
    }

    /**
     * @return the currentREQUEST
     */
    public REQUEST getCurrentREQUEST() {
        return currentREQUEST;
    }

    /**
     * @return the ready
     */
    public boolean isReady() {
        return ready;
    }

    /**
     * @param ready
     *            the ready to set
     */
    protected void setReady(final boolean ready) {
        this.ready = ready;
    }

}
