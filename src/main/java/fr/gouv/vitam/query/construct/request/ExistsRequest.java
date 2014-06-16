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

import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;

/**
 * @author "Frederic Bregier"
 *
 */
public class ExistsRequest extends Request {
	/**
	 * Exists, Missing, IsNull Request constructor
	 * @param existsRequest exists, missing, isNull
	 * @param variableName
	 * @throws InvalidCreateOperationException 
	 */
	public ExistsRequest(REQUEST existsRequest, String variableName) throws InvalidCreateOperationException {
		super();
		switch (existsRequest) {
			case exists:
			case isNull:
			case missing:
				break;
			default:
				throw new InvalidCreateOperationException("Request "+existsRequest+" is not an Exists/Missing/IsNull Request");
		}
		createRequestVariable(existsRequest, variableName);
		currentREQUEST = existsRequest;
		setReady(true);
	}
}
