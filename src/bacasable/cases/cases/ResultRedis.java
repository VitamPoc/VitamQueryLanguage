/**
 * This file is part of POC MongoDB ElasticSearch Project.
 *
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * POC MongoDB ElasticSearch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with POC MongoDB ElasticSearch . If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.cases;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.GlobalDatas;
import fr.gouv.vitam.utils.json.JsonHandler;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * Result (potentially cached) object
 *
 * @author "Frederic Bregier"
 *
 */
public class ResultRedis extends ResultAbstract {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(ResultRedis.class);

    /**
     * Id of the result
     */
    public String id = null;

    protected ObjectNode node = JsonHandler.createObjectNode();
    
    /**
     *
     */
    public ResultRedis() {

    }

    /**
     * @param collection
     */
    public ResultRedis(final Collection<String> collection) {
        currentDaip.addAll(collection);
        updateMinMax();
        // Path list so as loaded (never cached)
        loaded = true;
        putBeforeSave();
    }
    /**
     * Set a new ID
     *
     * @param id
     */
    public final void setId(final CassandraAccess dbvitam, final String id) {
        this.id = id;
    }
    /**
    *
    * @return the ID
    */
    public String getId() {
        return id;
    }
    /**
     * Put from argument
     * @param from
     */
    public void putFrom(final ResultInterface from) {
        node = ((ResultRedis) from).node.deepCopy();
        loaded = true;
        getAfterLoad();
    }

    /**
     * To be called after a load from Database
     */
    public void getAfterLoad() {
        if (node.has(CURRENTDAIP)) {
            final ArrayNode obj = (ArrayNode) node.withArray(CURRENTDAIP);
            final Set<String> vtset = new HashSet<String>();
            for (JsonNode string : obj) {
                vtset.add(string.asText());
            }
            currentDaip.clear();
            currentDaip.addAll(vtset);
        }
        minLevel = node.path(MINLEVEL).asInt(0);
        maxLevel = node.path(MAXLEVEL).asInt(0);
        nbSubNodes = node.path(NBSUBNODES).asLong(-1);
    }
    /**
     * To be called before save to database
     */
    public void putBeforeSave() {
        if (!currentDaip.isEmpty()) {
            ArrayNode array =  node.putArray(CURRENTDAIP);
            for (String string : currentDaip) {
                array.add(string);
            }
        }
        node.put(MINLEVEL, minLevel);
        node.put(MAXLEVEL, maxLevel);
        node.put(NBSUBNODES, nbSubNodes);
    }
    /**
     * Save to the Couchbase Database
     * @param dbvitam
     */
    public void save(final CassandraAccess dbvitam) {
        putBeforeSave();
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("SAVE: "+this);
        }
        if (id == null) {
            return;
        }
        dbvitam.ra.setToId(id, node, GlobalDatas.TTL);
        loaded = true;
    }
    /**
     * Load the object from the JsonNode
     * @param node
     */
    public void loadFromJson(JsonNode node) {
        this.node.setAll((ObjectNode) node);
        getAfterLoad();
        loaded = true;
    }
    /**
     * Update the TTL for this
     * @param dbvitam
     */
    public void updateTtl(final CassandraAccess dbvitam) {
        if (id == null) {
            return;
        }
        dbvitam.ra.updateTtl(id, GlobalDatas.TTL);
    }
}
