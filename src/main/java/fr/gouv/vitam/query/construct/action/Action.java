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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.UPDATE;
import fr.gouv.vitam.query.parser.ParserTokens.UPDATEARGS;
import fr.gouv.vitam.utils.json.JsonHandler;

/**
 * @author "Frederic Bregier"
 *
 */
public class Action {
    protected ObjectNode currentAction;
    protected JsonNode currentObject;
    protected UPDATE currentUPDATE;
    protected boolean ready;

    protected final void createActionArray(final UPDATE action) {
        currentObject = ((ObjectNode) currentObject).putArray(action.exactToken());
    }

    protected final void createActionVariableEach(final UPDATE action, final String variableName)
            throws InvalidCreateOperationException {
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Action " + action + " cannot be created with empty variable name");
        }
        currentObject = ((ObjectNode) currentObject).putObject(action.exactToken()).putObject(variableName.trim())
                .putArray(UPDATEARGS.each.exactToken());
    }

    protected final void createActionVariable(final UPDATE action, final String variableName)
            throws InvalidCreateOperationException {
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Action " + action + " cannot be created with empty variable name");
        }
        ((ObjectNode) currentObject).put(action.exactToken(), variableName.trim());
    }

    protected final void createActionVariables(final UPDATE action, final String... variableNames)
            throws InvalidCreateOperationException {
        final ArrayNode node = ((ObjectNode) currentObject).putArray(action.exactToken());
        for (final String var : variableNames) {
            if (var != null && !var.trim().isEmpty()) {
                node.add(var.trim());
            }
        }
        currentObject = node;
    }

    protected final void createActionVariableValue(final UPDATE action, final String variableName, final long value)
            throws InvalidCreateOperationException {
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Action " + action + " cannot be created with empty variable name");
        }
        currentObject = ((ObjectNode) currentObject).putObject(action.exactToken());
        ((ObjectNode) currentObject).put(variableName.trim(), value);
    }

    protected final void createActionVariableValue(final UPDATE action, final String variableName, final double value)
            throws InvalidCreateOperationException {
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Action " + action + " cannot be created with empty variable name");
        }
        currentObject = ((ObjectNode) currentObject).putObject(action.exactToken());
        ((ObjectNode) currentObject).put(variableName.trim(), value);
    }

    protected final void createActionVariableValue(final UPDATE action, final String variableName, final String value)
            throws InvalidCreateOperationException {
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Action " + action + " cannot be created with empty variable name");
        }
        currentObject = ((ObjectNode) currentObject).putObject(action.exactToken());
        ((ObjectNode) currentObject).put(variableName.trim(), value);
    }

    protected final void createActionVariableValue(final UPDATE action, final String variableName, final boolean value)
            throws InvalidCreateOperationException {
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new InvalidCreateOperationException("Action " + action + " cannot be created with empty variable name");
        }
        currentObject = ((ObjectNode) currentObject).putObject(action.exactToken());
        ((ObjectNode) currentObject).put(variableName.trim(), value);
    }

    /**
     * Empty constructor
     */
    protected Action() {
        currentAction = JsonHandler.createObjectNode();
        currentObject = currentAction;
        currentUPDATE = null;
        ready = false;
    }

    /**
     * Clean the object
     */
    protected void clean() {
    }

    /**
     * @return the currentAction
     */
    public ObjectNode getCurrentAction() {
        return currentAction;
    }

    /**
     * @return the currentObject
     */
    public JsonNode getCurrentObject() {
        return currentObject;
    }

    /**
     * @return the currentUPDATE
     */
    public UPDATE getCurrentUPDATE() {
        return currentUPDATE;
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
