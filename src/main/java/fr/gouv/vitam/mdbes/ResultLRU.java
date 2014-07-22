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
package fr.gouv.vitam.mdbes;

import java.util.Collection;

import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.lru.SynchronizedLruCache;

/**
 * Result (potentially cached) object
 *
 * @author "Frederic Bregier"
 *
 */
public class ResultLRU extends ResultAbstract {
    
    /**
     * Synchronized LRU cache
     */
    public static final SynchronizedLruCache<String, ResultLRU> LRU_ResultCached = new SynchronizedLruCache<String, ResultLRU>(1000000, GlobalDatas.TTLMS);

    /**
     * Id of the result
     */
    public String id;
    
    /**
     *
     */
    public ResultLRU() {

    }

    /**
     * @param collection
     */
    public ResultLRU(final Collection<String> collection) {
        currentDaip.addAll(collection);
        updateMinMax();
        // Path list so as loaded (never cached)
        loaded = true;
    }
    /**
     * Set a new ID
     *
     * @param id
     */
    public final void setId(final String id) {
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
        this.id = from.getId();
        this.currentDaip.clear();
        this.currentDaip.addAll(from.getCurrentDaip());
        this.maxLevel = from.getMaxLevel();
        this.minLevel = from.getMinLevel();
        this.nbSubNodes = from.getNbSubNodes();
        loaded = true;
    }
    
    /**
     * To be called after a load from Database
     */
    public void getAfterLoad() {
    }
    /**
     * To be called before save to database
     */
    public void putBeforeSave() {
    }
    /**
     * Save to the Couchbase Database
     * @param dbvitam
     */
    public void save(final MongoDbAccess dbvitam) {
        if (id == null) {
            return;
        }
        loaded = true;
        LRU_ResultCached.put(id, this);
    }
    /**
     * Update the TTL for this
     * @param dbvitam
     */
    public void updateTtl(final MongoDbAccess dbvitam) {
        if (id == null) {
            return;
        }
        LRU_ResultCached.updateTtl(id);
    }
    /**
     * *
     * @param id
     * @return True if the id is in the LRU
     */
    public static final boolean exists(String id) {
        if (id == null) {
            return false;
        }
        return LRU_ResultCached.contains(id);
    }
    /**
     * 
     * @return the number of element in the cache
     */
    public static final long count() {
        LRU_ResultCached.forceClearOldest();
        return LRU_ResultCached.size();
    }
}
