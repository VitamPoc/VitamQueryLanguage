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

import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;

/**
 * @author "Frederic Bregier"
 *
 */
public class RangeRequest extends Request {
    /**
     * Range Query constructor
     * 
     * @param variableName
     * @param from gt, gte
     * @param valueFrom
     * @param to lt, lte
     * @param valueTo
     * @throws InvalidCreateOperationException 
     * 
     */
    public RangeRequest(String variableName, REQUEST from, long valueFrom, REQUEST to, long valueTo) throws InvalidCreateOperationException {
        super();
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request "+currentREQUEST+" cannot be updated with empty variable name");
        }
        switch (from) {
            case gt:
            case gte:
                break;
            default:
                throw new InvalidCreateOperationException("Request "+from+" is not a valid Compare Request");
        }
        switch (to) {
            case lt:
            case lte:
                break;
            default:
                throw new InvalidCreateOperationException("Request "+to+" is not a valid Compare Request");
        }
        ObjectNode sub = ((ObjectNode) currentObject).putObject(REQUEST.range.exactToken()).putObject(variableName.trim());
        sub.put(from.exactToken(), valueFrom);
        sub.put(to.exactToken(), valueTo);
        currentREQUEST = REQUEST.range;
        setReady(true);
    }
    /**
     * Range Query constructor
     * 
     * @param variableName
     * @param from gt, gte
     * @param valueFrom
     * @param to lt, lte
     * @param valueTo
     * @throws InvalidCreateOperationException 
     */
    public RangeRequest(String variableName, REQUEST from, double valueFrom, REQUEST to, double valueTo) throws InvalidCreateOperationException {
        super();
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request "+currentREQUEST+" cannot be updated with empty variable name");
        }
        switch (from) {
            case gt:
            case gte:
                break;
            default:
                throw new InvalidCreateOperationException("Request "+from+" is not a valid Compare Request");
        }
        switch (to) {
            case lt:
            case lte:
                break;
            default:
                throw new InvalidCreateOperationException("Request "+to+" is not a valid Compare Request");
        }
        ObjectNode sub = ((ObjectNode) currentObject).putObject(REQUEST.range.exactToken()).putObject(variableName.trim());
        sub.put(from.exactToken(), valueFrom);
        sub.put(to.exactToken(), valueTo);
        currentREQUEST = REQUEST.range;
        setReady(true);
    }
    /**
     * Range Query constructor
     * 
     * @param variableName
     * @param from gt, gte
     * @param valueFrom
     * @param to lt, lte
     * @param valueTo
     * @throws InvalidCreateOperationException 
     */
    public RangeRequest(String variableName, REQUEST from, String valueFrom, REQUEST to, String valueTo) throws InvalidCreateOperationException {
        super();
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request "+currentREQUEST+" cannot be updated with empty variable name");
        }
        switch (from) {
            case gt:
            case gte:
                break;
            default:
                throw new InvalidCreateOperationException("Request "+from+" is not a valid Compare Request");
        }
        switch (to) {
            case lt:
            case lte:
                break;
            default:
                throw new InvalidCreateOperationException("Request "+to+" is not a valid Compare Request");
        }
        ObjectNode sub = ((ObjectNode) currentObject).putObject(REQUEST.range.exactToken()).putObject(variableName.trim());
        sub.put(from.exactToken(), valueFrom);
        sub.put(to.exactToken(), valueTo);
        currentREQUEST = REQUEST.range;
        setReady(true);
    }
}
