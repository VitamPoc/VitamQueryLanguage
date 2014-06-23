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

import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;
import fr.gouv.vitam.query.parser.ParserTokens.REQUESTARGS;

/**
 * @author "Frederic Bregier"
 *
 */
public class MatchRequest extends Request {
    /**
     * Match Request constructor
     * @param matchRequest match, match_phrase, match_phrase_prefix
     * @param variableName 
     * @param value 
     * @throws InvalidCreateOperationException 
     */
    public MatchRequest(REQUEST matchRequest, String variableName, String value) throws InvalidCreateOperationException {
        super();
        switch (matchRequest) {
            case match:
            case match_phrase:
            case match_phrase_prefix:
            case prefix: {
                createRequestVariableValue(matchRequest, variableName, value);
                currentREQUEST = matchRequest;
                setReady(true);
                break;
            }
            default:
                throw new InvalidCreateOperationException("Request "+matchRequest+" is not a Match Request");
        }
    }
    /**
     * 
     * @param max max expansions for Match type request only (not regex, search)
     * @return this MatchRequest
     * @throws InvalidCreateOperationException
     */
    public final MatchRequest setMatchMaxExpansions(int max) throws InvalidCreateOperationException {
        switch (currentREQUEST) {
            case match:
            case match_phrase:
            case match_phrase_prefix:
            case prefix:
                ((ObjectNode) currentObject).put(REQUESTARGS.max_expansions.exactToken(), max);
                break;
            default:
                throw new InvalidCreateOperationException("Request "+currentREQUEST+" is not a Match Request");
        }
        return this;
    }
}
