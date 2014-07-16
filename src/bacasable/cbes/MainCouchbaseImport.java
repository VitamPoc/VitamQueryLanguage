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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.bson.BSONObject;

import rx.Observable;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.QueryResult;
import com.mongodb.util.JSON;

import fr.gouv.vitam.mdbes.MainIngestFile;
import fr.gouv.vitam.utils.logging.LogbackLoggerFactory;
import fr.gouv.vitam.utils.logging.VitamLogLevel;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * @author "Frederic Bregier"
 *
 */
public class MainCouchbaseImport {
    private static VitamLogger LOGGER = null;
    
    
    /**
     * @param args 192.168.56.110 VitamLinks file
     */
    @SuppressWarnings("unchecked")
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
            String ingest = args[2];
            
            final long date11 = System.currentTimeMillis();
            long nb = 0;
            System.out.println("Starting: ");
            try {
                final FileInputStream fstream = new FileInputStream(ingest);
                final DataInputStream in = new DataInputStream(fstream);
                final BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String strLine;
                JsonObject content = JsonObject.empty();
                Map<String, Object> map = content.toMap();
                String id = null;
                // Read File Line By Line
                //List<Observable<JsonDocument>> list = new ArrayList<Observable<JsonDocument>>(500);
                Queue<Observable<JsonDocument>> queue = new LinkedList<Observable<JsonDocument>>();
                while ((strLine = br.readLine()) != null) {
                    final BSONObject bson = (BSONObject) JSON.parse(strLine);
                    map.clear();
                    map.putAll(bson.toMap());
                    id = (String) bson.get("_id");
                    JsonDocument doc = JsonDocument.create(id, content);
                    //bucket.upsert(doc);
                    nb++;
                    /*
                    if (nb % 500 == 0) {
                        for (Observable<JsonDocument> observable : list) {
                            observable.toBlockingObservable().single();
                        }
                        list.clear();
                        bucket.insert(doc).toBlockingObservable().single();
                        if (nb % 10000 == 0) {
                            System.out.print(".");
                        }
                    } else {
                        //bucket.insert(doc).toBlockingObservable().single();
                        list.add(bucket.insert(doc));
                    }
                    */
                    if (nb > 500) {
                        Observable<JsonDocument> observable = queue.poll();
                        if (observable != null) {
                            observable.toBlockingObservable().single();
                        }
                        queue.add(bucket.insert(doc));
                        if (nb % 10000 == 0) {
                            System.out.print(".");
                        }
                    } else {
                        queue.add(bucket.insert(doc));
                    }
                }
                /*
                for (Observable<JsonDocument> observable : list) {
                    observable.toBlockingObservable().single();
                }
                list.clear();
                */
                Observable<JsonDocument> observable = queue.poll();
                while (observable != null) {
                    observable.toBlockingObservable().single();
                    observable = queue.poll();
                }
                // Close the input stream
                in.close();
            } catch (IOException e) {
                LOGGER.error("IO", e);
            }
            final long date12 = System.currentTimeMillis();
            System.out.println("\nEnd");
            
            LOGGER.warn("IngestCouchBase: "+nb+" in "+(date12-date11)+" = "+((date12-date11)/((double) nb)));
            // now try requests
            executeQuery(bucket, "CREATE INDEX daip2daip_up ON VitamLinks(_up)");
            final long date13 = System.currentTimeMillis();
            executeQuery(bucket, "CREATE INDEX domain2daip_doms ON VitamLinks(_doms)");
            final long date14 = System.currentTimeMillis();
            executeQuery(bucket, "CREATE INDEX daip2paip_paip ON VitamLinks(_paip)");
            final long date15 = System.currentTimeMillis();
            executeQuery(bucket, "CREATE INDEX daip2dua_dua ON VitamLinks(_dua)");
            final long date16 = System.currentTimeMillis();
            executeQuery(bucket, "CREATE INDEX daip_dds ON VitamLinks(_dds)");
            final long date17 = System.currentTimeMillis();
            LOGGER.warn("SelectCouchBase: "+(date13-date12)+":"+(date14-date13)+":"+(date15-date14)
                    +":"+(date16-date15)+":"+(date17-date16));
            
        } catch (Exception e) {
            LOGGER.error("All", e);
        }
        cluster.disconnect();
        LOGGER.warn("Disconnected");
    }

    private final static void executeQuery(Bucket bucket, String request) {
        Iterator<QueryResult> iterator = bucket.query(Query.raw(request)).toBlockingObservable().getIterator();
        while (iterator.hasNext()) {
            QueryResult result = iterator.next();
            System.out.println(result);
        }
    }
}
