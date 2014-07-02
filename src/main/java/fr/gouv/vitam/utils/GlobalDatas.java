/**
   This file is part of POC MongoDB ElasticSearch Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   POC MongoDB ElasticSearch is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with POC MongoDB ElasticSearch .  If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.utils;

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
    public static boolean useFilter = true; // Should we use filter to select from graph parents, or within query
    public static boolean useNewNode = false;
    public static final String INDEXNAME = "vitamidx";
    public static long limitES = 10001; // limit before using ES in 1 level only
    public static int limitLoad = 10000; // limit for loading result
    public static final long LIMIT_ES_NEW_INDEX = 49999; // limit before flushing ES with Bulk
    public static final int MAXDEPTH = 100; // should be 20 but let a great margin
    public static final boolean PRINT_REQUEST = false;
    public static final boolean BLOCKING = true;
}
