/**
 * This file is part of POC MongoDB ElasticSearch Project.
 *
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 *
 * All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * POC MongoDB ElasticSearch is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with POC MongoDB
 * ElasticSearch . If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.mdbes;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author "Frederic Bregier"
 *
 */
public class BenchContext {
    /**
     * Last Counters as current status
     */
    public HashMap<String, AtomicLong> cpts = new HashMap<>();
    /**
     * Last named values as current status
     */
    public HashMap<String, String> savedNames = new HashMap<>();
    /**
     * Distribution counter
     */
    public AtomicLong distrib = null;
    /**
     * ES Index to be used 
     */
    public String indexName;
    /**
     * Type used in ES
     */
    public String typeName;
}
