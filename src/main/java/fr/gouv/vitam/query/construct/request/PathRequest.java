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

import com.fasterxml.jackson.databind.node.ArrayNode;

import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;

/**
 * @author "Frederic Bregier"
 *
 */
public class PathRequest extends Request {
	/**
	 * Path Request constructor
	 * @param pathes
	 * @throws InvalidCreateOperationException
	 */
	public PathRequest(String ... pathes) throws InvalidCreateOperationException {
		super();
		createRequestArray(REQUEST.path);
		ArrayNode array = ((ArrayNode) currentObject);
		for (String elt : pathes) {
			if (elt == null || elt.trim().isEmpty()) {
				continue;
			}
			array.add(elt.trim());
		}
		currentREQUEST = REQUEST.path;
		setReady(true);
	}
	/**
	 * Add other paths (at end) to a PATH Request
	 * @param pathes
	 * @return this PathRequest
	 * @throws InvalidCreateOperationException 
	 */
	public final PathRequest addPath(String ... pathes) throws InvalidCreateOperationException {
		if (currentREQUEST != REQUEST.path) {
			throw new InvalidCreateOperationException("Path cannot be added since this is not a path query: "+currentREQUEST);			
		}
		ArrayNode array = ((ArrayNode) currentObject);
		for (String elt : pathes) {
			if (elt == null || elt.trim().isEmpty()) {
				continue;
			}
			array.add(elt.trim());
		}
		return this;
	}
}
