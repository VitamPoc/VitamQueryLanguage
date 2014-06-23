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

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;
import fr.gouv.vitam.query.parser.ParserTokens.REQUESTARGS;

/**
 * @author "Frederic Bregier"
 *
 */
public class MltRequest extends Request {
    protected Set<String> stringVals;
    
    /**
     * Clean the object
     */
    protected void clean() {
        super.clean();
        stringVals = null;
    }
    /**
     * MoreLikeThis Request constructor
     * 
     * @param mltRequest flt, mlt
     * @param value 
     * @param variableNames 
     * @throws InvalidCreateOperationException 
     */
    public MltRequest(REQUEST mltRequest, String value, String ... variableNames) throws InvalidCreateOperationException {
        super();
        if (value == null || value.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Request "+mltRequest+" cannot be created with empty variable name");
        }
        switch (mltRequest) {
            case flt:
            case mlt: {
                ObjectNode sub = ((ObjectNode) currentObject).putObject(mltRequest.exactToken());
                ArrayNode array = sub.putArray(REQUESTARGS.fields.exactToken());
                stringVals = new HashSet<String>();
                for (String varName : variableNames) {
                    if (varName == null || varName.trim().isEmpty()) {
                        continue;
                    }
                    String var = varName.trim();
                    if (! stringVals.contains(var)) {
                        array.add(var);
                        stringVals.add(var);
                    }
                }
                currentObject = array;
                sub.put(REQUESTARGS.like.exactToken(), value);
                break;
            }
            default:
                throw new InvalidCreateOperationException("Request "+mltRequest+" is not an MoreLikeThis or In Request");
        }
        currentREQUEST = mltRequest;
        setReady(true);
    }
    /**
     * Add a variable into the Mlt Request
     * @param variableName
     * @return the MltRequest
     * @throws InvalidCreateOperationException 
     */
    public final MltRequest addMltVariable(String ...variableName) throws InvalidCreateOperationException {
        if (currentREQUEST != REQUEST.flt && currentREQUEST != REQUEST.mlt) {
            throw new InvalidCreateOperationException("Cannot add a variableName since this is not an Mlt Request: "+currentREQUEST);
        }
        ArrayNode array = (ArrayNode) currentObject;
        if (stringVals == null) {
            stringVals = new HashSet<String>();
        }
        for (String val : variableName) {
            if (val == null || val.trim().isEmpty()) {
                throw new InvalidCreateOperationException("Request "+currentREQUEST+" cannot be updated with empty variable name");
            }
            val = val.trim();
            if (! stringVals.contains(val)) {
                array.add(val);
                stringVals.add(val);
            }
        }
        return this;
    }
}
