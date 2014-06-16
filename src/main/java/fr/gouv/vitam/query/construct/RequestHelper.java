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

import java.util.Map;

import fr.gouv.vitam.query.construct.request.BooleanRequest;
import fr.gouv.vitam.query.construct.request.CompareRequest;
import fr.gouv.vitam.query.construct.request.ExistsRequest;
import fr.gouv.vitam.query.construct.request.InRequest;
import fr.gouv.vitam.query.construct.request.MatchRequest;
import fr.gouv.vitam.query.construct.request.MltRequest;
import fr.gouv.vitam.query.construct.request.PathRequest;
import fr.gouv.vitam.query.construct.request.RangeRequest;
import fr.gouv.vitam.query.construct.request.SearchRequest;
import fr.gouv.vitam.query.construct.request.TermRequest;
import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;

/**
 * @author "Frederic Bregier"
 *
 */
public class RequestHelper {
	public static final PathRequest path(String ...pathes) throws InvalidCreateOperationException {
		return new PathRequest(pathes);
	}
	public static final BooleanRequest and() throws InvalidCreateOperationException {
		return new BooleanRequest(REQUEST.and);
	}
	public static final BooleanRequest or() throws InvalidCreateOperationException {
		return new BooleanRequest(REQUEST.or);
	}
	public static final BooleanRequest not() throws InvalidCreateOperationException {
		return new BooleanRequest(REQUEST.not);
	}
	public static final BooleanRequest nor() throws InvalidCreateOperationException {
		return new BooleanRequest(REQUEST.nor);
	}
	public static final CompareRequest eq(String variableName, boolean value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.eq, variableName, value);
	}
	public static final CompareRequest eq(String variableName, long value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.eq, variableName, value);
	}
	public static final CompareRequest eq(String variableName, double value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.eq, variableName, value);
	}
	public static final CompareRequest eq(String variableName, String value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.eq, variableName, value);
	}
	public static final CompareRequest ne(String variableName, boolean value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.ne, variableName, value);
	}
	public static final CompareRequest ne(String variableName, long value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.ne, variableName, value);
	}
	public static final CompareRequest ne(String variableName, double value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.ne, variableName, value);
	}
	public static final CompareRequest ne(String variableName, String value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.ne, variableName, value);
	}
	public static final CompareRequest lt(String variableName, boolean value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.lt, variableName, value);
	}
	public static final CompareRequest lt(String variableName, long value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.lt, variableName, value);
	}
	public static final CompareRequest lt(String variableName, double value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.lt, variableName, value);
	}
	public static final CompareRequest lt(String variableName, String value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.lt, variableName, value);
	}
	public static final CompareRequest lte(String variableName, boolean value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.lte, variableName, value);
	}
	public static final CompareRequest lte(String variableName, long value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.lte, variableName, value);
	}
	public static final CompareRequest lte(String variableName, double value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.lte, variableName, value);
	}
	public static final CompareRequest lte(String variableName, String value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.lte, variableName, value);
	}
	public static final CompareRequest gt(String variableName, boolean value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.gt, variableName, value);
	}
	public static final CompareRequest gt(String variableName, long value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.gt, variableName, value);
	}
	public static final CompareRequest gt(String variableName, double value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.gt, variableName, value);
	}
	public static final CompareRequest gt(String variableName, String value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.gt, variableName, value);
	}
	public static final CompareRequest gte(String variableName, boolean value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.gte, variableName, value);
	}
	public static final CompareRequest gte(String variableName, long value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.gte, variableName, value);
	}
	public static final CompareRequest gte(String variableName, double value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.gte, variableName, value);
	}
	public static final CompareRequest gte(String variableName, String value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.gte, variableName, value);
	}
	public static final CompareRequest size(String variableName, long value) throws InvalidCreateOperationException {
		return new CompareRequest(REQUEST.size, variableName, value);
	}
	public static final ExistsRequest exists(String variableName) throws InvalidCreateOperationException {
		return new ExistsRequest(REQUEST.exists, variableName);
	}
	public static final ExistsRequest missing(String variableName) throws InvalidCreateOperationException {
		return new ExistsRequest(REQUEST.missing, variableName);
	}
	public static final ExistsRequest isNull(String variableName) throws InvalidCreateOperationException {
		return new ExistsRequest(REQUEST.isNull, variableName);
	}
	public static final InRequest in(String variableName, boolean ...value) throws InvalidCreateOperationException {
		return new InRequest(REQUEST.in, variableName, value);
	}
	public static final InRequest in(String variableName, long ...value) throws InvalidCreateOperationException {
		return new InRequest(REQUEST.in, variableName, value);
	}
	public static final InRequest in(String variableName, double ...value) throws InvalidCreateOperationException {
		return new InRequest(REQUEST.in, variableName, value);
	}
	public static final InRequest in(String variableName, String ...value) throws InvalidCreateOperationException {
		return new InRequest(REQUEST.in, variableName, value);
	}
	public static final InRequest nin(String variableName, boolean ...value) throws InvalidCreateOperationException {
		return new InRequest(REQUEST.nin, variableName, value);
	}
	public static final InRequest nin(String variableName, long ...value) throws InvalidCreateOperationException {
		return new InRequest(REQUEST.nin, variableName, value);
	}
	public static final InRequest nin(String variableName, double ...value) throws InvalidCreateOperationException {
		return new InRequest(REQUEST.nin, variableName, value);
	}
	public static final InRequest nin(String variableName, String ...value) throws InvalidCreateOperationException {
		return new InRequest(REQUEST.nin, variableName, value);
	}
	public static final MatchRequest match(String variableName, String value) throws InvalidCreateOperationException {
		return new MatchRequest(REQUEST.match, variableName, value);
	}
	public static final MatchRequest matchPhrase(String variableName, String value) throws InvalidCreateOperationException {
		return new MatchRequest(REQUEST.match_phrase, variableName, value);
	}
	public static final MatchRequest matchPhrasePrefix(String variableName, String value) throws InvalidCreateOperationException {
		return new MatchRequest(REQUEST.match_phrase_prefix, variableName, value);
	}
	public static final MatchRequest prefix(String variableName, String value) throws InvalidCreateOperationException {
		return new MatchRequest(REQUEST.prefix, variableName, value);
	}
	public static final SearchRequest regex(String variableName, String value) throws InvalidCreateOperationException {
		return new SearchRequest(REQUEST.regex, variableName, value);
	}
	public static final SearchRequest search(String variableName, String value) throws InvalidCreateOperationException {
		return new SearchRequest(REQUEST.search, variableName, value);
	}
	public static final TermRequest term(String variableName, String value) throws InvalidCreateOperationException {
		return new TermRequest(variableName, value);
	}
	public static final TermRequest term(Map<String, String> variableNameValue) throws InvalidCreateOperationException {
		return new TermRequest(variableNameValue);
	}
	public static final MltRequest flt(String value, String ...variableName) throws InvalidCreateOperationException {
		return new MltRequest(REQUEST.flt, value, variableName);
	}
	public static final MltRequest mlt(String value, String ...variableName) throws InvalidCreateOperationException {
		return new MltRequest(REQUEST.mlt, value, variableName);
	}
	public static final RangeRequest range(String variableName, long min, boolean includeMin, long max, boolean includeMax) throws InvalidCreateOperationException {
		REQUEST rmin = includeMin ? REQUEST.gte : REQUEST.gt;
		REQUEST rmax = includeMax ? REQUEST.lte : REQUEST.lt;
		return new RangeRequest(variableName, rmin, min, rmax, max);
	}
	public static final RangeRequest range(String variableName, double min, boolean includeMin, double max, boolean includeMax) throws InvalidCreateOperationException {
		REQUEST rmin = includeMin ? REQUEST.gte : REQUEST.gt;
		REQUEST rmax = includeMax ? REQUEST.lte : REQUEST.lt;
		return new RangeRequest(variableName, rmin, min, rmax, max);
	}
	public static final RangeRequest range(String variableName, String min, boolean includeMin, String max, boolean includeMax) throws InvalidCreateOperationException {
		REQUEST rmin = includeMin ? REQUEST.gte : REQUEST.gt;
		REQUEST rmax = includeMax ? REQUEST.lte : REQUEST.lt;
		return new RangeRequest(variableName, rmin, min, rmax, max);
	}
}
