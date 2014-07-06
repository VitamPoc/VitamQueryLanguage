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

import com.fasterxml.jackson.databind.node.ArrayNode;

import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.UPDATE;

/**
 * @author "Frederic Bregier"
 *
 */
public class PopAction extends Action {
    /**
     * Pop Action constructor
     *
     * @param variableName
     * @param value
     * @throws InvalidCreateOperationException
     */
    public PopAction(final String variableName, final String... value) throws InvalidCreateOperationException {
        super();
        createActionVariableEach(UPDATE.pop, variableName);
        for (final String val : value) {
            if (val != null && !val.trim().isEmpty()) {
                ((ArrayNode) currentObject).add(val.trim());
            }
        }
        currentUPDATE = UPDATE.pop;
        setReady(true);
    }

    /**
     * Pop Action constructor
     *
     * @param variableName
     * @param value
     * @throws InvalidCreateOperationException
     */
    public PopAction(final String variableName, final long... value) throws InvalidCreateOperationException {
        super();
        createActionVariableEach(UPDATE.pop, variableName);
        for (final long val : value) {
            ((ArrayNode) currentObject).add(val);
        }
        currentUPDATE = UPDATE.pop;
        setReady(true);
    }

    /**
     * Pop Action constructor
     *
     * @param variableName
     * @param value
     * @throws InvalidCreateOperationException
     */
    public PopAction(final String variableName, final boolean... value) throws InvalidCreateOperationException {
        super();
        createActionVariableEach(UPDATE.pop, variableName);
        for (final boolean val : value) {
            ((ArrayNode) currentObject).add(val);
        }
        currentUPDATE = UPDATE.pop;
        setReady(true);
    }

    /**
     * Pop Action constructor
     *
     * @param variableName
     * @param value
     * @throws InvalidCreateOperationException
     */
    public PopAction(final String variableName, final double... value) throws InvalidCreateOperationException {
        super();
        createActionVariableEach(UPDATE.pop, variableName);
        for (final double val : value) {
            ((ArrayNode) currentObject).add(val);
        }
        currentUPDATE = UPDATE.pop;
        setReady(true);
    }

    /**
     * Add other Pop sub actions to Pop Request
     *
     * @param variableName
     * @param value
     * @return the PopAction
     * @throws InvalidCreateOperationException
     */
    public final PopAction addPopAction(final String... value) throws InvalidCreateOperationException {
        if (currentUPDATE != UPDATE.pop) {
            throw new InvalidCreateOperationException("Cannot add a set element since this is not a Pop Action: " + currentUPDATE);
        }
        for (final String val : value) {
            if (val != null && !val.trim().isEmpty()) {
                ((ArrayNode) currentObject).add(val.trim());
            }
        }
        return this;
    }

    /**
     * Add other Pop sub actions to Pop Request
     *
     * @param variableName
     * @param value
     * @return the PopAction
     * @throws InvalidCreateOperationException
     */
    public final PopAction addPopAction(final boolean... value) throws InvalidCreateOperationException {
        if (currentUPDATE != UPDATE.pop) {
            throw new InvalidCreateOperationException("Cannot add a set element since this is not a Pop Action: " + currentUPDATE);
        }
        for (final boolean val : value) {
            ((ArrayNode) currentObject).add(val);
        }
        return this;
    }

    /**
     * Add other Pop sub actions to Pop Request
     *
     * @param variableName
     * @param value
     * @return the PopAction
     * @throws InvalidCreateOperationException
     */
    public final PopAction addPopAction(final long... value) throws InvalidCreateOperationException {
        if (currentUPDATE != UPDATE.pop) {
            throw new InvalidCreateOperationException("Cannot add a set element since this is not a Pop Action: " + currentUPDATE);
        }
        for (final long val : value) {
            ((ArrayNode) currentObject).add(val);
        }
        return this;
    }

    /**
     * Add other Pop sub actions to Pop Request
     *
     * @param variableName
     * @param value
     * @return the PopAction
     * @throws InvalidCreateOperationException
     */
    public final PopAction addPopAction(final double... value) throws InvalidCreateOperationException {
        if (currentUPDATE != UPDATE.pop) {
            throw new InvalidCreateOperationException("Cannot add a set element since this is not a Pop Action: " + currentUPDATE);
        }
        for (final double val : value) {
            ((ArrayNode) currentObject).add(val);
        }
        return this;
    }

}
