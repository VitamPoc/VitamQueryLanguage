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
package fr.gouv.vitam.query.construct;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.construct.action.Action;
import fr.gouv.vitam.query.construct.request.Request;
import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.json.JsonHandler;
import fr.gouv.vitam.query.parser.ParserTokens.ACTIONFILTER;
import fr.gouv.vitam.query.parser.ParserTokens.GLOBAL;

/**
 * @author "Frederic Bregier"
 *
 */
public class Update {
	protected ArrayNode requests;
	protected ObjectNode filter;
	protected ArrayNode actions;
	/**
	 * 
	 * @return this Update
	 */
	public final Update resetFilter() {
		if (filter != null) {
			filter.removeAll();
		}
		return this;
	}
	/**
	 * 
	 * @return this Update
	 */
	public final Update resetAction() {
		if (actions != null) {
			actions.removeAll();
		}
		return this;
	}
	/**
	 * 
	 * @return this Update
	 */
	public final Update resetRequests() {
		if (requests != null) {
			requests.removeAll();
		}
		return this;
	}
	/**
	 * @param mult True to act on multiple elements, False to act only on 1 element
	 * @return this Update
	 */
	public final Update setMult(boolean mult) {
		if (filter == null) {
			filter = JsonHandler.createObjectNode();
		}
		filter.put(ACTIONFILTER.mult.exactToken(), mult);
		return this;
	}

	/**
	 * 
	 * @param action
	 * @return this Update
	 */
	public final Update addActions(Action ... action) {
		if (this.actions == null) {
			this.actions = JsonHandler.createArrayNode();
		}
		for (Action act : action) {
			if (act.getCurrentAction() != null) {
				this.actions.add(act.getCurrentAction());
			}
		}
		return this;
	}

	/**
	 * 
	 * @param requests
	 * @return this Update
	 * @throws InvalidCreateOperationException 
	 */
	public final Update addRequests(Request ... requests) throws InvalidCreateOperationException {
		if (this.requests == null) {
			this.requests = JsonHandler.createArrayNode();
		}
		for (Request request : requests) {
			if (! request.isReady()) {
				throw new InvalidCreateOperationException("Request is not ready to be added: "+request.getCurrentRequest());
			}
			this.requests.add(request.getCurrentRequest());
		}
		return this;
	}
	/**
	 * 
	 * @return the Final Update containing all 3 parts: requests array, filter and actions
	 */
	public final ObjectNode getFinalUpdate() {
		ObjectNode node = JsonHandler.createObjectNode();
		if (requests != null && requests.size() > 0) {
			node.set(GLOBAL.query.exactToken(), requests);
		}
		if (filter != null && filter.size() > 0) {
			node.set(GLOBAL.filter.exactToken(), filter);
		}
		if (actions != null && actions.size() > 0) {
			node.set(GLOBAL.action.exactToken(), actions);
		}
		return node;
	}
	/**
	 * @return the requests array
	 */
	public final ArrayNode getRequests() {
		if (requests == null) {
			return JsonHandler.createArrayNode();
		}
		return requests;
	}
	/**
	 * @return the filter
	 */
	public final ObjectNode getFilter() {
		if (filter == null) {
			return JsonHandler.createObjectNode();
		}
		return filter;
	}
	/**
	 * @return the actions
	 */
	public final ArrayNode getActions() {
		if (actions == null) {
			return JsonHandler.createArrayNode();
		}
		return actions;
	}
}
