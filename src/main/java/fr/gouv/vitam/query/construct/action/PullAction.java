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

import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.UPDATE;

/**
 * @author "Frederic Bregier"
 *
 */
public class PullAction extends Action {
    /**
     * Pull Action constructor
     * @param variableName 
     * @param value 
     * @throws InvalidCreateOperationException 
     */
    public PullAction(String variableName, long value) throws InvalidCreateOperationException {
        super();
        createActionVariableValue(UPDATE.pull, variableName, value);
        currentUPDATE = UPDATE.pull;
        setReady(true);
    }
    /**
     * Pull Action constructor from variable name only (value to 1)
     * @param variableName
     * @throws InvalidCreateOperationException 
     */
    public PullAction(String variableName) throws InvalidCreateOperationException {
        this(variableName, 1);
    }

}
