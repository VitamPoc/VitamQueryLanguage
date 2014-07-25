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

import java.util.Date;
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
import fr.gouv.vitam.query.construct.request.WildcardRequest;
import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;

/**
 * @author "Frederic Bregier"
 *
 */
public final class RequestHelper {
    private RequestHelper() {
        // empty
    }

    /**
     *
     * @param pathes
     *            primary list of path in the future PathRequest
     * @return a PathRequest
     * @throws InvalidCreateOperationException
     */
    public static final PathRequest path(final String... pathes) throws InvalidCreateOperationException {
        return new PathRequest(pathes);
    }

    /**
     *
     * @return a BooleanRequest for AND operator
     * @throws InvalidCreateOperationException
     */
    public static final BooleanRequest and() throws InvalidCreateOperationException {
        return new BooleanRequest(REQUEST.and);
    }

    /**
     *
     * @return a BooleanRequest for OR operator
     * @throws InvalidCreateOperationException
     */
    public static final BooleanRequest or() throws InvalidCreateOperationException {
        return new BooleanRequest(REQUEST.or);
    }

    /**
     *
     * @return a BooleanRequest for NOT operator (using AND internally)
     * @throws InvalidCreateOperationException
     */
    public static final BooleanRequest not() throws InvalidCreateOperationException {
        return new BooleanRequest(REQUEST.not);
    }

    /**
     *
     * @return a BooleanRequest for NOR operator
     * @throws InvalidCreateOperationException
     */
    public static final BooleanRequest nor() throws InvalidCreateOperationException {
        return new BooleanRequest(REQUEST.nor);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using EQ comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest eq(final String variableName, final boolean value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.eq, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using EQ comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest eq(final String variableName, final long value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.eq, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using EQ comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest eq(final String variableName, final double value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.eq, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using EQ comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest eq(final String variableName, final String value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.eq, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using EQ comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest eq(final String variableName, final Date value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.eq, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using NE (non equal) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest ne(final String variableName, final boolean value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.ne, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using NE (non equal) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest ne(final String variableName, final long value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.ne, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using NE (non equal) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest ne(final String variableName, final double value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.ne, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using NE (non equal) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest ne(final String variableName, final String value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.ne, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using NE (non equal) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest ne(final String variableName, final Date value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.ne, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using LT (less than) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest lt(final String variableName, final boolean value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.lt, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using LT (less than) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest lt(final String variableName, final long value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.lt, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using LT (less than) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest lt(final String variableName, final double value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.lt, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using LT (less than) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest lt(final String variableName, final String value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.lt, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using LT (less than) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest lt(final String variableName, final Date value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.lt, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using LTE (less than or equal) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest lte(final String variableName, final boolean value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.lte, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using LTE (less than or equal) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest lte(final String variableName, final long value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.lte, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using LTE (less than or equal) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest lte(final String variableName, final double value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.lte, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using LTE (less than or equal) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest lte(final String variableName, final String value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.lte, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using LTE (less than or equal) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest lte(final String variableName, final Date value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.lte, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using GT (greater than) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest gt(final String variableName, final boolean value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.gt, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using GT (greater than) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest gt(final String variableName, final long value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.gt, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using GT (greater than) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest gt(final String variableName, final double value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.gt, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using GT (greater than) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest gt(final String variableName, final String value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.gt, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using GT (greater than) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest gt(final String variableName, final Date value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.gt, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using GTE (greater than or equal) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest gte(final String variableName, final boolean value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.gte, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using GTE (greater than or equal) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest gte(final String variableName, final long value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.gte, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using GTE (greater than or equal) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest gte(final String variableName, final double value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.gte, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using GTE (greater than or equal) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest gte(final String variableName, final String value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.gte, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using GTE (greater than or equal) comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest gte(final String variableName, final Date value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.gte, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a CompareRequest using SIZE comparator
     * @throws InvalidCreateOperationException
     */
    public static final CompareRequest size(final String variableName, final long value) throws InvalidCreateOperationException {
        return new CompareRequest(REQUEST.size, variableName, value);
    }

    /**
     *
     * @param variableName
     * @return an ExistsRequest
     * @throws InvalidCreateOperationException
     *             using Exists operator
     */
    public static final ExistsRequest exists(final String variableName) throws InvalidCreateOperationException {
        return new ExistsRequest(REQUEST.exists, variableName);
    }

    /**
     *
     * @param variableName
     * @return an ExistsRequest using Missing operator
     * @throws InvalidCreateOperationException
     */
    public static final ExistsRequest missing(final String variableName) throws InvalidCreateOperationException {
        return new ExistsRequest(REQUEST.missing, variableName);
    }

    /**
     *
     * @param variableName
     * @return an ExistsRequest using isNull operator
     * @throws InvalidCreateOperationException
     */
    public static final ExistsRequest isNull(final String variableName) throws InvalidCreateOperationException {
        return new ExistsRequest(REQUEST.isNull, variableName);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return an InRequest using IN operator
     * @throws InvalidCreateOperationException
     */
    public static final InRequest in(final String variableName, final boolean... value) throws InvalidCreateOperationException {
        return new InRequest(REQUEST.in, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return an InRequest using IN operator
     * @throws InvalidCreateOperationException
     */
    public static final InRequest in(final String variableName, final long... value) throws InvalidCreateOperationException {
        return new InRequest(REQUEST.in, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return an InRequest using IN operator
     * @throws InvalidCreateOperationException
     */
    public static final InRequest in(final String variableName, final double... value) throws InvalidCreateOperationException {
        return new InRequest(REQUEST.in, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return an InRequest using IN operator
     * @throws InvalidCreateOperationException
     */
    public static final InRequest in(final String variableName, final String... value) throws InvalidCreateOperationException {
        return new InRequest(REQUEST.in, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return an InRequest using IN operator
     * @throws InvalidCreateOperationException
     */
    public static final InRequest in(final String variableName, final Date... value) throws InvalidCreateOperationException {
        return new InRequest(REQUEST.in, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return an InRequest using NIN (not in) operator
     * @throws InvalidCreateOperationException
     */
    public static final InRequest nin(final String variableName, final boolean... value) throws InvalidCreateOperationException {
        return new InRequest(REQUEST.nin, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return an InRequest using NIN (not in) operator
     * @throws InvalidCreateOperationException
     */
    public static final InRequest nin(final String variableName, final long... value) throws InvalidCreateOperationException {
        return new InRequest(REQUEST.nin, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return an InRequest using NIN (not in) operator
     * @throws InvalidCreateOperationException
     */
    public static final InRequest nin(final String variableName, final double... value) throws InvalidCreateOperationException {
        return new InRequest(REQUEST.nin, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return an InRequest using NIN (not in) operator
     * @throws InvalidCreateOperationException
     */
    public static final InRequest nin(final String variableName, final String... value) throws InvalidCreateOperationException {
        return new InRequest(REQUEST.nin, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return an InRequest using NIN (not in) operator
     * @throws InvalidCreateOperationException
     */
    public static final InRequest nin(final String variableName, final Date... value) throws InvalidCreateOperationException {
        return new InRequest(REQUEST.nin, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a MatchRequest using MATCH operator
     * @throws InvalidCreateOperationException
     */
    public static final MatchRequest match(final String variableName, final String value) throws InvalidCreateOperationException {
        return new MatchRequest(REQUEST.match, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a MatchRequest using MATCH_PHRASE operator
     * @throws InvalidCreateOperationException
     */
    public static final MatchRequest matchPhrase(final String variableName, final String value)
            throws InvalidCreateOperationException {
        return new MatchRequest(REQUEST.match_phrase, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a MatchRequest using MATCH_PHRASE_PREFIX operator
     * @throws InvalidCreateOperationException
     */
    public static final MatchRequest matchPhrasePrefix(final String variableName, final String value)
            throws InvalidCreateOperationException {
        return new MatchRequest(REQUEST.match_phrase_prefix, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a MatchRequest using PREFIX operator
     * @throws InvalidCreateOperationException
     */
    public static final MatchRequest prefix(final String variableName, final String value) throws InvalidCreateOperationException {
        return new MatchRequest(REQUEST.prefix, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a SearchRequest using REGEX operator
     * @throws InvalidCreateOperationException
     */
    public static final SearchRequest regex(final String variableName, final String value) throws InvalidCreateOperationException {
        return new SearchRequest(REQUEST.regex, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a SearchRequest using SEARCH operator
     * @throws InvalidCreateOperationException
     */
    public static final SearchRequest search(final String variableName, final String value)
            throws InvalidCreateOperationException {
        return new SearchRequest(REQUEST.search, variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a TermRequest
     * @throws InvalidCreateOperationException
     */
    public static final TermRequest term(final String variableName, final String value) throws InvalidCreateOperationException {
        return new TermRequest(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a TermRequest
     * @throws InvalidCreateOperationException
     */
    public static final TermRequest term(final String variableName, final long value) throws InvalidCreateOperationException {
        return new TermRequest(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a TermRequest
     * @throws InvalidCreateOperationException
     */
    public static final TermRequest term(final String variableName, final double value) throws InvalidCreateOperationException {
        return new TermRequest(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a TermRequest
     * @throws InvalidCreateOperationException
     */
    public static final TermRequest term(final String variableName, final boolean value) throws InvalidCreateOperationException {
        return new TermRequest(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a TermRequest
     * @throws InvalidCreateOperationException
     */
    public static final TermRequest term(final String variableName, final Date value) throws InvalidCreateOperationException {
        return new TermRequest(variableName, value);
    }

    /**
     *
     * @param variableNameValue
     *            Map of VariableName of Value
     * @return a TermRequest
     * @throws InvalidCreateOperationException
     */
    public static final TermRequest term(final Map<String, Object> variableNameValue) throws InvalidCreateOperationException {
        return new TermRequest(variableNameValue);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a WildcardRequest
     * @throws InvalidCreateOperationException
     */
    public static final WildcardRequest wildcard(final String variableName, final String value) throws InvalidCreateOperationException {
        return new WildcardRequest(variableName, value);
    }

    /**
     *
     * @param value
     * @param variableName
     * @return a MltRequest using a FLT (fuzzy like this) operator
     * @throws InvalidCreateOperationException
     */
    public static final MltRequest flt(final String value, final String... variableName) throws InvalidCreateOperationException {
        return new MltRequest(REQUEST.flt, value, variableName);
    }

    /**
     *
     * @param value
     * @param variableName
     * @return a MltRequest using a MLT (more like this) operator
     * @throws InvalidCreateOperationException
     */
    public static final MltRequest mlt(final String value, final String... variableName) throws InvalidCreateOperationException {
        return new MltRequest(REQUEST.mlt, value, variableName);
    }

    /**
     *
     * @param variableName
     * @param min
     * @param includeMin
     * @param max
     * @param includeMax
     * @return a RangeRequest
     * @throws InvalidCreateOperationException
     */
    public static final RangeRequest range(final String variableName, final long min, final boolean includeMin, final long max,
            final boolean includeMax) throws InvalidCreateOperationException {
        final REQUEST rmin = includeMin ? REQUEST.gte : REQUEST.gt;
        final REQUEST rmax = includeMax ? REQUEST.lte : REQUEST.lt;
        return new RangeRequest(variableName, rmin, min, rmax, max);
    }

    /**
     *
     * @param variableName
     * @param min
     * @param includeMin
     * @param max
     * @param includeMax
     * @return a RangeRequest
     * @throws InvalidCreateOperationException
     */
    public static final RangeRequest range(final String variableName, final double min, final boolean includeMin,
            final double max, final boolean includeMax) throws InvalidCreateOperationException {
        final REQUEST rmin = includeMin ? REQUEST.gte : REQUEST.gt;
        final REQUEST rmax = includeMax ? REQUEST.lte : REQUEST.lt;
        return new RangeRequest(variableName, rmin, min, rmax, max);
    }

    /**
     *
     * @param variableName
     * @param min
     * @param includeMin
     * @param max
     * @param includeMax
     * @return a RangeRequest
     * @throws InvalidCreateOperationException
     */
    public static final RangeRequest range(final String variableName, final String min, final boolean includeMin,
            final String max, final boolean includeMax) throws InvalidCreateOperationException {
        final REQUEST rmin = includeMin ? REQUEST.gte : REQUEST.gt;
        final REQUEST rmax = includeMax ? REQUEST.lte : REQUEST.lt;
        return new RangeRequest(variableName, rmin, min, rmax, max);
    }

    /**
     *
     * @param variableName
     * @param min
     * @param includeMin
     * @param max
     * @param includeMax
     * @return a RangeRequest
     * @throws InvalidCreateOperationException
     */
    public static final RangeRequest range(final String variableName, final Date min, final boolean includeMin,
            final Date max, final boolean includeMax) throws InvalidCreateOperationException {
        final REQUEST rmin = includeMin ? REQUEST.gte : REQUEST.gt;
        final REQUEST rmax = includeMax ? REQUEST.lte : REQUEST.lt;
        return new RangeRequest(variableName, rmin, min, rmax, max);
    }
}
