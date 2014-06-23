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
package fr.gouv.vitam.query.construct.action;

import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.UPDATE;

/**
 * @author "Frederic Bregier"
 *
 */
public class SetAction extends Action {
    /**
     * Set Action constructor
     * @param variableName 
     * @param value 
     * @throws InvalidCreateOperationException 
     */
    public SetAction(String variableName, String value) throws InvalidCreateOperationException {
        super();
        createActionVariableValue(UPDATE.set, variableName, value);
        currentUPDATE = UPDATE.set;
        setReady(true);
    }
    /**
     * Set Action constructor from Map
     * @param variableNameValue
     */
    public SetAction(Map<String, ?> variableNameValue) {
        super();
        currentObject = ((ObjectNode) currentObject).putObject(UPDATE.set.exactToken());
        ObjectNode node = (ObjectNode) currentObject;
        for (Entry<String, ?> entry : variableNameValue.entrySet()) {
            String name = entry.getKey();
            if (name == null || name.trim().isEmpty()) {
                continue;
            }
            Object val = entry.getValue();
            if (val instanceof String) {
                node.put(name.trim(), (String) val);
            } else if (val instanceof Long) {
                node.put(name.trim(), (Long) val);
            } else if (val instanceof Integer) {
                node.put(name.trim(), (Integer) val);
            } else if (val instanceof Double) {
                node.put(name.trim(), (Double) val);
            } else if (val instanceof Float) {
                node.put(name.trim(), (Float) val);
            } else if (val instanceof Boolean) {
                node.put(name.trim(), (Boolean) val);
            } else {
                node.put(name.trim(), val.toString());
            }
        }
        currentUPDATE = UPDATE.set;
        setReady(true);
    }
    /**
     * Set Action constructor
     * @param variableName 
     * @param value 
     * @throws InvalidCreateOperationException 
     */
    public SetAction(String variableName, long value) throws InvalidCreateOperationException {
        super();
        createActionVariableValue(UPDATE.set, variableName, value);
        currentUPDATE = UPDATE.set;
        setReady(true);
    }
    /**
     * Set Action constructor
     * @param variableName 
     * @param value 
     * @throws InvalidCreateOperationException 
     */
    public SetAction(String variableName, boolean value) throws InvalidCreateOperationException {
        super();
        createActionVariableValue(UPDATE.set, variableName, value);
        currentUPDATE = UPDATE.set;
        setReady(true);
    }
    /**
     * Set Action constructor
     * @param variableName 
     * @param value 
     * @throws InvalidCreateOperationException 
     */
    public SetAction(String variableName, double value) throws InvalidCreateOperationException {
        super();
        createActionVariableValue(UPDATE.set, variableName, value);
        currentUPDATE = UPDATE.set;
        setReady(true);
    }
    /**
     * Add other Set sub actions to Set Request
     * @param variableName
     * @param value
     * @return the SetAction
     * @throws InvalidCreateOperationException 
     */
    public final SetAction addSetAction(String variableName, String value) throws InvalidCreateOperationException {
        if (currentUPDATE != UPDATE.set) {
            throw new InvalidCreateOperationException("Cannot add a set element since this is not a Set Action: "+currentUPDATE);
        }
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Action "+currentUPDATE+" cannot be updated with empty variable name");
        }
        ((ObjectNode) currentObject).put(variableName.trim(), value);
        return this;
    }
    /**
     * Add other Set sub actions to Set Request
     * @param variableName
     * @param value
     * @return the SetAction
     * @throws InvalidCreateOperationException 
     */
    public final SetAction addSetAction(String variableName, boolean value) throws InvalidCreateOperationException {
        if (currentUPDATE != UPDATE.set) {
            throw new InvalidCreateOperationException("Cannot add a set element since this is not a Set Action: "+currentUPDATE);
        }
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Action "+currentUPDATE+" cannot be updated with empty variable name");
        }
        ((ObjectNode) currentObject).put(variableName.trim(), value);
        return this;
    }
    /**
     * Add other Set sub actions to Set Request
     * @param variableName
     * @param value
     * @return the SetAction
     * @throws InvalidCreateOperationException 
     */
    public final SetAction addSetAction(String variableName, long value) throws InvalidCreateOperationException {
        if (currentUPDATE != UPDATE.set) {
            throw new InvalidCreateOperationException("Cannot add a set element since this is not a Set Action: "+currentUPDATE);
        }
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Action "+currentUPDATE+" cannot be updated with empty variable name");
        }
        ((ObjectNode) currentObject).put(variableName.trim(), value);
        return this;
    }
    /**
     * Add other Set sub actions to Set Request
     * @param variableName
     * @param value
     * @return the SetAction
     * @throws InvalidCreateOperationException 
     */
    public final SetAction addSetAction(String variableName, double value) throws InvalidCreateOperationException {
        if (currentUPDATE != UPDATE.set) {
            throw new InvalidCreateOperationException("Cannot add a set element since this is not a Set Action: "+currentUPDATE);
        }
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Action "+currentUPDATE+" cannot be updated with empty variable name");
        }
        ((ObjectNode) currentObject).put(variableName.trim(), value);
        return this;
    }
}
