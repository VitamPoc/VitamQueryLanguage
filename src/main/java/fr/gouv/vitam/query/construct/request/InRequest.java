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
import java.util.HashSet;
import java.util.Set;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.json.JsonHandler;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;

/**
 * In and Nin requests
 *
 * @author "Frederic Bregier"
 *
 */
public class InRequest extends Request {
    protected Set<Boolean> booleanVals;
    protected Set<Long> longVals;
    protected Set<Double> doubleVals;
    protected Set<String> stringVals;

    /**
     * Clean the object
     */
    @Override
    protected void clean() {
        super.clean();
        booleanVals = null;
        longVals = null;
        doubleVals = null;
        stringVals = null;
    }

    /**
     * In Request constructor
     *
     * @param inRequest
     *            in, nin
     * @param variableName
     * @param value
     * @throws InvalidCreateOperationException
     */
    public InRequest(final REQUEST inRequest, final String variableName, final long value) throws InvalidCreateOperationException {
        super();
        switch (inRequest) {
            case in:
            case nin: {
                if (variableName == null || variableName.trim().isEmpty()) {
                    throw new InvalidCreateOperationException("Request " + inRequest
                            + " cannot be created with empty variable name");
                }
                final ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
                final ArrayNode array = sub.putArray(variableName.trim());
                array.add(value);
                longVals = new HashSet<Long>();
                longVals.add(value);
                currentObject = array;
                currentREQUEST = inRequest;
                setReady(true);
                break;
            }
            default:
                throw new InvalidCreateOperationException("Request " + inRequest + " is not an In Request");
        }
    }

    /**
     * In Request constructor
     *
     * @param inRequest
     *            in, nin
     * @param variableName
     * @param value
     * @throws InvalidCreateOperationException
     */
    public InRequest(final REQUEST inRequest, final String variableName, final double value)
            throws InvalidCreateOperationException {
        super();
        switch (inRequest) {
            case in:
            case nin: {
                if (variableName == null || variableName.trim().isEmpty()) {
                    throw new InvalidCreateOperationException("Request " + inRequest
                            + " cannot be created with empty variable name");
                }
                final ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
                final ArrayNode array = sub.putArray(variableName.trim());
                array.add(value);
                doubleVals = new HashSet<Double>();
                doubleVals.add(value);
                currentObject = array;
                currentREQUEST = inRequest;
                setReady(true);
                break;
            }
            default:
                throw new InvalidCreateOperationException("Request " + inRequest + " is not an In Request");
        }
    }

    /**
     * In Request constructor
     *
     * @param inRequest
     *            in, nin
     * @param variableName
     * @param value
     * @throws InvalidCreateOperationException
     */
    public InRequest(final REQUEST inRequest, final String variableName, final String value)
            throws InvalidCreateOperationException {
        super();
        switch (inRequest) {
            case in:
            case nin: {
                if (variableName == null || variableName.trim().isEmpty()) {
                    throw new InvalidCreateOperationException("Request " + inRequest
                            + " cannot be created with empty variable name");
                }
                final ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
                final ArrayNode array = sub.putArray(variableName.trim());
                array.add(value);
                stringVals = new HashSet<String>();
                stringVals.add(value);
                currentObject = array;
                currentREQUEST = inRequest;
                setReady(true);
                break;
            }
            default:
                throw new InvalidCreateOperationException("Request " + inRequest + " is not an In or Search Request");
        }
    }

    /**
     * In Request constructor
     *
     * @param inRequest
     *            in, nin
     * @param variableName
     * @param value
     * @throws InvalidCreateOperationException
     */
    public InRequest(final REQUEST inRequest, final String variableName, final Date value)
            throws InvalidCreateOperationException {
        super();
        switch (inRequest) {
            case in:
            case nin: {
                if (variableName == null || variableName.trim().isEmpty()) {
                    throw new InvalidCreateOperationException("Request " + inRequest
                            + " cannot be created with empty variable name");
                }
                final ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
                final ArrayNode array = sub.putArray(variableName.trim());
                final DateTime dateTime = new DateTime(value);
                final String sdate = dateTime.toString();
                ObjectNode elt = JsonHandler.createObjectNode().put(DATE, sdate);
                array.add(elt);
                stringVals = new HashSet<String>();
                stringVals.add(sdate);
                currentObject = array;
                currentREQUEST = inRequest;
                setReady(true);
                break;
            }
            default:
                throw new InvalidCreateOperationException("Request " + inRequest + " is not an In or Search Request");
        }
    }

    /**
     * In Request constructor
     *
     * @param inRequest
     *            in, nin
     * @param variableName
     * @param value
     * @throws InvalidCreateOperationException
     */
    public InRequest(final REQUEST inRequest, final String variableName, final boolean value)
            throws InvalidCreateOperationException {
        super();
        switch (inRequest) {
            case in:
            case nin: {
                if (variableName == null || variableName.trim().isEmpty()) {
                    throw new InvalidCreateOperationException("Request " + inRequest
                            + " cannot be created with empty variable name");
                }
                final ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
                final ArrayNode array = sub.putArray(variableName.trim());
                array.add(value);
                booleanVals = new HashSet<Boolean>();
                booleanVals.add(value);
                currentObject = array;
                currentREQUEST = inRequest;
                setReady(true);
                break;
            }
            default:
                throw new InvalidCreateOperationException("Request " + inRequest + " is not an In Request");
        }
    }

    /**
     * In Request constructor
     *
     * @param inRequest
     *            in, nin
     * @param variable
     * @param values
     * @throws InvalidCreateOperationException
     */
    public InRequest(final REQUEST inRequest, final String variable, final String... values)
            throws InvalidCreateOperationException {
        super();
        if (variable == null || variable.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request " + inRequest + " cannot be created with empty variable name");
        }
        switch (inRequest) {
            case in:
            case nin: {
                final ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
                final ArrayNode array = sub.putArray(variable.trim());
                stringVals = new HashSet<String>();
                for (final String value : values) {
                    if (!stringVals.contains(value)) {
                        array.add(value);
                        stringVals.add(value);
                    }
                }
                currentObject = array;
                break;
            }
            default:
                throw new InvalidCreateOperationException("Request " + inRequest + " is not an In Request");
        }
        currentREQUEST = inRequest;
        setReady(true);
    }

    /**
     * In Request constructor
     *
     * @param inRequest
     *            in, nin
     * @param variable
     * @param values
     * @throws InvalidCreateOperationException
     */
    public InRequest(final REQUEST inRequest, final String variable, final Date... values)
            throws InvalidCreateOperationException {
        super();
        if (variable == null || variable.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request " + inRequest + " cannot be created with empty variable name");
        }
        switch (inRequest) {
            case in:
            case nin: {
                final ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
                final ArrayNode array = sub.putArray(variable.trim());
                stringVals = new HashSet<String>();
                for (final Date value : values) {
                    final DateTime dateTime = new DateTime(value);
                    final String sdate = dateTime.toString();
                    if (!stringVals.contains(sdate)) {
                        ObjectNode elt = JsonHandler.createObjectNode().put(DATE, sdate);
                        array.add(elt);
                        stringVals.add(sdate);
                    }
                }
                currentObject = array;
                break;
            }
            default:
                throw new InvalidCreateOperationException("Request " + inRequest + " is not an In Request");
        }
        currentREQUEST = inRequest;
        setReady(true);
    }

    /**
     * In Request constructor
     *
     * @param inRequest
     *            in, nin
     * @param variableName
     * @param values
     * @throws InvalidCreateOperationException
     */
    public InRequest(final REQUEST inRequest, final String variableName, final long... values)
            throws InvalidCreateOperationException {
        super();
        if (inRequest != REQUEST.in && inRequest != REQUEST.nin) {
            throw new InvalidCreateOperationException("Request " + inRequest + " is not an In Request");
        }
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request " + currentREQUEST + " cannot be created with empty variable name");
        }
        final ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
        final ArrayNode array = sub.putArray(variableName.trim());
        longVals = new HashSet<Long>();
        for (final long value : values) {
            if (!longVals.contains(value)) {
                array.add(value);
                longVals.add(value);
            }
        }
        currentObject = array;
        currentREQUEST = inRequest;
        setReady(true);
    }

    /**
     * In Request constructor
     *
     * @param inRequest
     *            in, nin
     * @param variableName
     * @param values
     * @throws InvalidCreateOperationException
     */
    public InRequest(final REQUEST inRequest, final String variableName, final double... values)
            throws InvalidCreateOperationException {
        super();
        if (inRequest != REQUEST.in && inRequest != REQUEST.nin) {
            throw new InvalidCreateOperationException("Request " + inRequest + " is not an In Request");
        }
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request " + currentREQUEST + " cannot be created with empty variable name");
        }
        final ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
        final ArrayNode array = sub.putArray(variableName.trim());
        doubleVals = new HashSet<Double>();
        for (final double value : values) {
            if (!doubleVals.contains(value)) {
                array.add(value);
                doubleVals.add(value);
            }
        }
        currentObject = array;
        currentREQUEST = inRequest;
        setReady(true);
    }

    /**
     * In Request constructor
     *
     * @param inRequest
     *            in, nin
     * @param variableName
     * @param values
     * @throws InvalidCreateOperationException
     */
    public InRequest(final REQUEST inRequest, final String variableName, final boolean... values)
            throws InvalidCreateOperationException {
        super();
        if (inRequest != REQUEST.in && inRequest != REQUEST.nin) {
            throw new InvalidCreateOperationException("Request " + inRequest + " is not an In Request");
        }
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request " + currentREQUEST + " cannot be created with empty variable name");
        }
        final ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
        final ArrayNode array = sub.putArray(variableName.trim());
        booleanVals = new HashSet<Boolean>();
        for (final boolean value : values) {
            if (!booleanVals.contains(value)) {
                array.add(value);
                booleanVals.add(value);
            }
        }
        currentObject = array;
        currentREQUEST = inRequest;
        setReady(true);
    }

    /**
     * Add an In Value to an existing In Request
     *
     * @param inValue
     * @return the InRequest
     * @throws InvalidCreateOperationException
     */
    public final InRequest addInValue(final String... inValue) throws InvalidCreateOperationException {
        if (currentREQUEST != REQUEST.in && currentREQUEST != REQUEST.nin) {
            throw new InvalidCreateOperationException("Cannot add an InValue since this is not an In Request: " + currentREQUEST);
        }
        final ArrayNode array = (ArrayNode) currentObject;
        if (stringVals == null) {
            stringVals = new HashSet<String>();
        }
        for (final String val : inValue) {
            if (!stringVals.contains(val)) {
                array.add(val);
                stringVals.add(val);
            }
        }
        return this;
    }

    /**
     * Add an In Value to an existing In Request
     *
     * @param inValue
     * @return the InRequest
     * @throws InvalidCreateOperationException
     */
    public final InRequest addInValue(final Date... inValue) throws InvalidCreateOperationException {
        if (currentREQUEST != REQUEST.in && currentREQUEST != REQUEST.nin) {
            throw new InvalidCreateOperationException("Cannot add an InValue since this is not an In Request: " + currentREQUEST);
        }
        final ArrayNode array = (ArrayNode) currentObject;
        if (stringVals == null) {
            stringVals = new HashSet<String>();
        }
        for (final Date val : inValue) {
            final DateTime dateTime = new DateTime(val);
            final String sdate = dateTime.toString();
            if (!stringVals.contains(sdate)) {
                ObjectNode elt = JsonHandler.createObjectNode().put(DATE, sdate);
                array.add(elt);
                stringVals.add(sdate);
            }
        }
        return this;
    }

    /**
     * Add an In Value to an existing In Request
     *
     * @param inValue
     * @return the InRequest
     * @throws InvalidCreateOperationException
     */
    public final InRequest addInValue(final long... inValue) throws InvalidCreateOperationException {
        if (currentREQUEST != REQUEST.in && currentREQUEST != REQUEST.nin) {
            throw new InvalidCreateOperationException("Cannot add an InValue since this is not an In Request: " + currentREQUEST);
        }
        final ArrayNode array = (ArrayNode) currentObject;
        if (longVals == null) {
            longVals = new HashSet<Long>();
        }
        for (final long l : inValue) {
            if (!longVals.contains(l)) {
                array.add(l);
                longVals.add(l);
            }
        }
        return this;
    }

    /**
     * Add an In Value to an existing In Request
     *
     * @param inValue
     * @return the InRequest
     * @throws InvalidCreateOperationException
     */
    public final InRequest addInValue(final double... inValue) throws InvalidCreateOperationException {
        if (currentREQUEST != REQUEST.in && currentREQUEST != REQUEST.nin) {
            throw new InvalidCreateOperationException("Cannot add an InValue since this is not an In Request: " + currentREQUEST);
        }
        final ArrayNode array = (ArrayNode) currentObject;
        if (doubleVals == null) {
            doubleVals = new HashSet<Double>();
        }
        for (final double d : inValue) {
            if (!doubleVals.contains(d)) {
                array.add(d);
                doubleVals.add(d);
            }
        }
        return this;
    }

    /**
     * Add an In Value to an existing In Request
     *
     * @param inValue
     * @return the InRequest
     * @throws InvalidCreateOperationException
     */
    public final InRequest addInValue(final boolean... inValue) throws InvalidCreateOperationException {
        if (currentREQUEST != REQUEST.in && currentREQUEST != REQUEST.nin) {
            throw new InvalidCreateOperationException("Cannot add an InValue since this is not an In Request: " + currentREQUEST);
        }
        final ArrayNode array = (ArrayNode) currentObject;
        if (booleanVals == null) {
            booleanVals = new HashSet<Boolean>();
        }
        for (final boolean b : inValue) {
            if (!booleanVals.contains(b)) {
                array.add(b);
                booleanVals.add(b);
            }
        }
        return this;
    }
}
