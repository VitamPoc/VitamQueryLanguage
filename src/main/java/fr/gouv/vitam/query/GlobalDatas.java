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
package fr.gouv.vitam.query;

import java.util.HashSet;
import java.util.Set;

/**
 * @author "Frederic Bregier"
 *
 */
public final class GlobalDatas {
    private GlobalDatas() {
        // empty
    }

    /**
     * set of Roots Domain : must be updated each time a new Domain is created
     */
    public static final Set<String> ROOTS = new HashSet<>();
    /**
     * Should we use filter to select from graph parents, or within query (filtering seems better)
     */
    public static boolean useFilter = true;
    /**
     * Shall we use FilteredRequest or postFilter for requests
     */
    public static boolean useFilteredRequest = true;
    /**
     * Default behavior of ElasticSearch connection (False tends to minimize the number of concurrent connections)
     */
    public static boolean useNewNode = false;
    /**
     * Default Index name for ElasticSearch
     */
    public static final String INDEXNAME = "vitamidx";
    /**
     * Default local address to be used by the client (null means no specific address)
     */
    public static String localNetworkAddress = null;
    /**
     * limit before using ES in 1 level only (set to -1 means use ES at all time)
     */
    public static long limitES = 10001;
    /**
     * limit for loading result
     */
    public static int limitLoad = 10000;
    /**
     * limit before flushing ES with Bulk
     */
    public static final int LIMIT_ES_NEW_INDEX = 10000;
    /**
     * limit before flushing MongoDB with Bulk
     */
    public static final int LIMIT_MDB_NEW_INDEX = 10000;
    /**
     * should be 20 but let a great margin
     */
    public static final int MAXDEPTH = 100;
    /**
     * In Debug mode : shall we print the request
     */
    public static final boolean PRINT_REQUEST = false;
    /**
     * Shall new entries insertion in the ElasticSearch index be in blocking mode
     */
    public static final boolean BLOCKING = false;
    /**
     * Shall we save ResultCache
     */
    public static final boolean SAVERESULT = true;
    /**
     * Default TTL : 24H
     */
    public static final int TTL = 3600*24;
    /**
     * Default TTL in ms : 24H
     */
    public static final int TTLMS = 3600*24*1000;
    /**
     * In conjunction with Couchbase (if USECOUCHBASE is False, it is ignored)
     */
    public static final boolean USEMEMCACHED = true;
    /**
     * Shall we use SynchronizedLruCache for cache of results
     */
    public static final boolean USELRUCACHE = false;
    /**
     * Shall we use Redis for cache of results
     */
    public static final boolean USEREDIS = true;
}
