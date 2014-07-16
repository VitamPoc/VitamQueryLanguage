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

package fr.gouv.vitam.cbes;

import java.util.Iterator;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.QueryResult;

import fr.gouv.vitam.mdbes.MainIngestFile;
import fr.gouv.vitam.utils.logging.LogbackLoggerFactory;
import fr.gouv.vitam.utils.logging.VitamLogLevel;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * @author "Frederic Bregier"
 *
 */
public class MainCouchbaseSelect {
    private static VitamLogger LOGGER = null;
    
    
    /**
     * @param args 192.168.56.110 VitamLinks file
     */
    public static void main(String[] args) {
        VitamLoggerFactory.setDefaultFactory(new LogbackLoggerFactory(VitamLogLevel.WARN));
        LOGGER = VitamLoggerFactory.getInstance(MainIngestFile.class);
        if (args.length < 3) {
            LOGGER.error("Need hostname bucketName");
            return;
        }
        String host = args[0];
        String bucketname = args[1];
        System.setProperty("com.couchbase.client.queryEnabled", "true");
        
        CouchbaseCluster cluster = new CouchbaseCluster(host);
        try {
            Bucket bucket = cluster.openBucket(bucketname).toBlockingObservable().single();
            if (bucket == null) {
                LOGGER.error("no bucket");
                return;
            }
            String metarequest = args[2];
            
            final long date11 = System.currentTimeMillis();
            executeQuery(bucket, "SELECT * FROM VitamLinks WHERE ANY val IN _up SATISFIES val='ohw_OTbYACd71LMAAUcSADIP' ");
            final long date12 = System.currentTimeMillis();
            // now try requests

            executeQuery(bucket, "SELECT * FROM VitamLinks WHERE _up='bIZPOTbYACd71LMAAUcSAEZx'");
            final long date13 = System.currentTimeMillis();
            executeQuery(bucket, "SELECT * FROM VitamLinks WHERE _up='ohw_OTbYACd71LMAAUcSADIP'");
            final long date14 = System.currentTimeMillis();
            executeQuery(bucket, "EXPLAIN SELECT * FROM VitamLinks WHERE _dds.ohw_OTbYACd71LMAAUcSADIP IS NOT MISSING AND _dds.ohw_OTbYACd71LMAAUcSADIP=1");
            executeQuery(bucket, "SELECT * FROM VitamLinks WHERE _dds.ohw_OTbYACd71LMAAUcSADIP IS NOT MISSING AND _dds.ohw_OTbYACd71LMAAUcSADIP=1 LIMIT 10");
            final long date15 = System.currentTimeMillis();

            LOGGER.warn("SelectCouchBase: "+(date11-date11)+":"+(date13-date12)+":"+(date14-date13)+":"+(date15-date14));
            
        } catch (Exception e) {
            LOGGER.error("All", e);
        }
        cluster.disconnect();
        LOGGER.warn("Disconnected");
    }

    private final static void executeQuery(Bucket bucket, String request) {
        Iterator<QueryResult> iterator = bucket.query(Query.raw(request)).toBlockingObservable().getIterator();
        System.out.println(request+"=>");
        while (iterator.hasNext()) {
            QueryResult result = iterator.next();
            System.out.println(result);
        }
    }
}
