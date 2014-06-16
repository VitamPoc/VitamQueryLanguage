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
 * Boolean Requests
 * @author "Frederic Bregier"
 *
 */
public class BooleanRequest extends Request {
	/**
	 * BooleanRequest constructor
	 * @param booleanRequest and or not nor
	 * @throws InvalidCreateOperationException
	 */
	public BooleanRequest(REQUEST booleanRequest) throws InvalidCreateOperationException {
		super();
		switch (booleanRequest) {
			case and:
			case nor:
			case not:
			case or: {
				if (currentObject.isArray()) {
					currentObject = ((ArrayNode) currentObject).addObject();
				}
				createRequestArray(booleanRequest);
				currentREQUEST = booleanRequest;
				break;
			}
			default:
				throw new InvalidCreateOperationException("Request "+booleanRequest+" is not a Boolean Request");
		}
	}
	/**
	 * Add sub requests to Boolean Request
	 * @param requests
	 * @return the BooleanRequest
	 * @throws InvalidCreateOperationException 
	 */
	public final BooleanRequest addToBooleanRequest(Request ... requests) throws InvalidCreateOperationException {
		if (currentREQUEST != null) {
			switch (currentREQUEST) {
				case and:
				case nor:
				case not:
				case or:
					break;
				default:
					throw new InvalidCreateOperationException("Requests cannot be added since this is not a boolean query: "+currentREQUEST);
			}
		}
		ArrayNode array = ((ArrayNode) currentObject);
		for (Request elt : requests) {
			if (! elt.isReady()) {
				throw new InvalidCreateOperationException("Requests cannot be added since not ready: "+elt.getCurrentRequest());
			}
			// in case sub query has those element set: not allowed
			elt.clean();
			array.add(elt.getCurrentRequest());
		}
		setReady(true);
		return this;
	}
}
