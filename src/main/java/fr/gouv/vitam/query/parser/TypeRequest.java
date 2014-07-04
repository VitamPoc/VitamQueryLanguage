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
package fr.gouv.vitam.query.parser;

import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;

/**
 * A Type of Request class
 * @author "Frederic Bregier"
 *
 */
public class TypeRequest {
	/**
	 * The type of Request
	 */
    public REQUEST type;
    /**
     * Relative depth
     */
    public int depth = 1;
    /**
     * Exact depth
     */
    public int exactdepth = 0;
    /**
     * Is this request a Depth request (relative or absolute)
     */
    public boolean isDepth = false;
    /**
     * Is this request an ElasticSearch only request
     */
    public boolean isOnlyES;
    /**
     * In case of Path request, the list of IDs
     */
    public List<String> refId;
    /**
     * The Couchbase related request
     */
    public String requestCb;
    /**
     * Request Model: MongoDB or ElasticSearch
     */
    public ObjectNode []requestModel;
    /**
     * Filter model: shall be used only to filter on "parent" relation and some others
     */
    public ObjectNode []filterModel;

    /**
     * 
     * @param nbModel the number of model supported
     */
    public TypeRequest(int nbModel) {
        isOnlyES = false;
        requestModel = new ObjectNode[nbModel];
        filterModel = new ObjectNode[nbModel];
    }
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(" Type: "+type+":"+refId);
        builder.append(" Depth: "+isDepth+":"+depth+":"+exactdepth);
        builder.append(" isOnlyES: "+isOnlyES);
        for (ObjectNode o : filterModel) {
            builder.append("\n\tfilter: "+o);
        }
        for (ObjectNode o : requestModel) {
            builder.append("\n\trequest: "+o);
        }
        builder.append("\n\trequest: "+requestCb);
        return builder.toString();
    }
}