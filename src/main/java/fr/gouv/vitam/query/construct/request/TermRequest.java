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
import java.util.Map;
import java.util.Map.Entry;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;

/**
 * @author "Frederic Bregier"
 *
 */
public class TermRequest extends Request {
    /**
     * Term Request constructor
     *
     * @param variableName
     * @param value
     * @throws InvalidCreateOperationException
     */
    public TermRequest(final String variableName, final String value) throws InvalidCreateOperationException {
        super();
        createRequestVariableValue(REQUEST.term, variableName, value);
        currentREQUEST = REQUEST.term;
        setReady(true);
    }
    /**
     * Term Request constructor
     *
     * @param variableName
     * @param value
     * @throws InvalidCreateOperationException
     */
    public TermRequest(final String variableName, final long value) throws InvalidCreateOperationException {
        super();
        createRequestVariableValue(REQUEST.term, variableName, value);
        currentREQUEST = REQUEST.term;
        setReady(true);
    }
    /**
     * Term Request constructor
     *
     * @param variableName
     * @param value
     * @throws InvalidCreateOperationException
     */
    public TermRequest(final String variableName, final double value) throws InvalidCreateOperationException {
        super();
        createRequestVariableValue(REQUEST.term, variableName, value);
        currentREQUEST = REQUEST.term;
        setReady(true);
    }
    /**
     * Term Request constructor
     *
     * @param variableName
     * @param value
     * @throws InvalidCreateOperationException
     */
    public TermRequest(final String variableName, final boolean value) throws InvalidCreateOperationException {
        super();
        createRequestVariableValue(REQUEST.term, variableName, value);
        currentREQUEST = REQUEST.term;
        setReady(true);
    }
    /**
     * Term Request constructor
     *
     * @param variableName
     * @param value
     * @throws InvalidCreateOperationException
     */
    public TermRequest(final String variableName, final Date value) throws InvalidCreateOperationException {
        super();
        createRequestVariableValue(REQUEST.term, variableName, value);
        currentREQUEST = REQUEST.term;
        setReady(true);
    }

    /**
     * Term Request constructor from Map
     *
     * @param variableNameValue
     */
    public TermRequest(final Map<String, Object> variableNameValue) {
        super();
        currentObject = ((ObjectNode) currentObject).putObject(REQUEST.term.exactToken());
        final ObjectNode node = (ObjectNode) currentObject;
        for (final Entry<String, Object> entry : variableNameValue.entrySet()) {
            final String name = entry.getKey();
            if (name == null || name.trim().isEmpty()) {
                continue;
            }
            Object val = entry.getValue();
            if (val instanceof Boolean) {
                node.put(name.trim(), (Boolean) val);
            } else if (val instanceof Long) {
                node.put(name.trim(), (Long) val);
            } else if (val instanceof Double) {
                node.put(name.trim(), (Double) val);
            } else if (val instanceof Date) {
                final DateTime dateTime = new DateTime(val);
                node.putObject(name.trim()).put(DATE, dateTime.toString());
            } else {
                node.put(name.trim(), val.toString());
            }
        }
        currentREQUEST = REQUEST.term;
        setReady(true);
    }

    /**
     * Add other Term sub requests to Term Request
     *
     * @param variableName
     * @param value
     * @return the TermRequest
     * @throws InvalidCreateOperationException
     */
    public final TermRequest addTermRequest(final String variableName, final String value) throws InvalidCreateOperationException {
        if (currentREQUEST != REQUEST.term) {
            throw new InvalidCreateOperationException("Cannot add a term element since this is not a Term Request: "
                    + currentREQUEST);
        }
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request " + currentREQUEST + " cannot be updated with empty variable name");
        }
        ((ObjectNode) currentObject).put(variableName.trim(), value);
        return this;
    }

    /**
     * Add other Term sub requests to Term Request
     *
     * @param variableName
     * @param value
     * @return the TermRequest
     * @throws InvalidCreateOperationException
     */
    public final TermRequest addTermRequest(final String variableName, final long value) throws InvalidCreateOperationException {
        if (currentREQUEST != REQUEST.term) {
            throw new InvalidCreateOperationException("Cannot add a term element since this is not a Term Request: "
                    + currentREQUEST);
        }
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request " + currentREQUEST + " cannot be updated with empty variable name");
        }
        ((ObjectNode) currentObject).put(variableName.trim(), value);
        return this;
    }

    /**
     * Add other Term sub requests to Term Request
     *
     * @param variableName
     * @param value
     * @return the TermRequest
     * @throws InvalidCreateOperationException
     */
    public final TermRequest addTermRequest(final String variableName, final double value) throws InvalidCreateOperationException {
        if (currentREQUEST != REQUEST.term) {
            throw new InvalidCreateOperationException("Cannot add a term element since this is not a Term Request: "
                    + currentREQUEST);
        }
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request " + currentREQUEST + " cannot be updated with empty variable name");
        }
        ((ObjectNode) currentObject).put(variableName.trim(), value);
        return this;
    }

    /**
     * Add other Term sub requests to Term Request
     *
     * @param variableName
     * @param value
     * @return the TermRequest
     * @throws InvalidCreateOperationException
     */
    public final TermRequest addTermRequest(final String variableName, final boolean value) throws InvalidCreateOperationException {
        if (currentREQUEST != REQUEST.term) {
            throw new InvalidCreateOperationException("Cannot add a term element since this is not a Term Request: "
                    + currentREQUEST);
        }
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request " + currentREQUEST + " cannot be updated with empty variable name");
        }
        ((ObjectNode) currentObject).put(variableName.trim(), value);
        return this;
    }

    /**
     * Add other Term sub requests to Term Request
     *
     * @param variableName
     * @param value
     * @return the TermRequest
     * @throws InvalidCreateOperationException
     */
    public final TermRequest addTermRequest(final String variableName, final Date value) throws InvalidCreateOperationException {
        if (currentREQUEST != REQUEST.term) {
            throw new InvalidCreateOperationException("Cannot add a term element since this is not a Term Request: "
                    + currentREQUEST);
        }
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request " + currentREQUEST + " cannot be updated with empty variable name");
        }
        final DateTime dateTime = new DateTime(value);
        ((ObjectNode) currentObject).putObject(variableName.trim()).put(DATE, dateTime.toString());
        return this;
    }

}
