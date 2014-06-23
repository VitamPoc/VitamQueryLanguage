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
public class UnsetAction extends Action {
    /**
     * UnSet Action constructor from list of variable names
     * @param variableNames
     * @throws InvalidCreateOperationException 
     */
    public UnsetAction(String ...variableNames) throws InvalidCreateOperationException {
        super();
        createActionVariables(UPDATE.unset, variableNames);
        currentUPDATE = UPDATE.unset;
        setReady(true);
    }
    /**
     * Add other UnSet sub actions to UnSet Request
     * @param variableNames
     * @return the UnSetAction
     * @throws InvalidCreateOperationException 
     */
    public final UnsetAction addUnSetAction(String ... variableNames) throws InvalidCreateOperationException {
        if (currentUPDATE != UPDATE.unset) {
            throw new InvalidCreateOperationException("Cannot add an unset element since this is not a UnSet Action: "+currentUPDATE);
        }
        for (String name : variableNames) {
            if (name != null && ! name.trim().isEmpty()) {
                ((ArrayNode) currentObject).add(name);
            }
        }
        return this;
    }

}
