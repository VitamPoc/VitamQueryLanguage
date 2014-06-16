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

import java.util.Map;
import java.util.Map.Entry;

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
	 * @param variableName 
	 * @param value 
	 * @throws InvalidCreateOperationException 
	 */
	public TermRequest(String variableName, String value) throws InvalidCreateOperationException {
		super();
		createRequestVariableValue(REQUEST.term, variableName, value);
		currentREQUEST = REQUEST.term;
		setReady(true);
	}
	/**
	 * Term Request constructor from Map
	 * @param variableNameValue
	 */
	public TermRequest(Map<String, String> variableNameValue) {
		super();
		currentObject = ((ObjectNode) currentObject).putObject(REQUEST.term.exactToken());
		ObjectNode node = (ObjectNode) currentObject;
		for (Entry<String, String> entry : variableNameValue.entrySet()) {
			String name = entry.getKey();
			if (name == null || name.trim().isEmpty()) {
				continue;
			}
			node.put(name.trim(), entry.getValue());
		}
		currentREQUEST = REQUEST.term;
		setReady(true);
	}
	/**
	 * Add other Term sub requests to Term Request
	 * @param variableName
	 * @param value
	 * @return the TermRequest
	 * @throws InvalidCreateOperationException 
	 */
	public final TermRequest addTermRequest(String variableName, String value) throws InvalidCreateOperationException {
		if (currentREQUEST != REQUEST.term) {
			throw new InvalidCreateOperationException("Cannot add a term element since this is not a Term Request: "+currentREQUEST);
		}
		if (variableName == null || variableName.trim().isEmpty()) {
			throw new InvalidCreateOperationException("Request "+currentREQUEST+" cannot be updated with empty variable name");
		}
		((ObjectNode) currentObject).put(variableName.trim(), value);
		return this;
	}

}
