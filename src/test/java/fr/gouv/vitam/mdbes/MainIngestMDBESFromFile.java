/**
 * This file is part of POC MongoDB ElasticSearch Project.
 *
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 *
 * All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either versionRank 3
 * of the License, or (at your option) any later versionRank.
 *
 * POC MongoDB ElasticSearch is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with POC MongoDB ElasticSearch . If not, see
 * <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.mdbes;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.PropertyConfigurator;
import org.bson.BSONObject;

import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

import fr.gouv.vitam.query.exception.InvalidUuidOperationException;
import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.logging.LogbackLoggerFactory;
import fr.gouv.vitam.utils.logging.VitamLogLevel;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * Load ingest file into MongoDB
 *
 * @author "Frederic Bregier"
 *
 */
@SuppressWarnings("javadoc")
public class MainIngestMDBESFromFile implements Runnable {
    private static VitamLogger LOGGER = null;
    
    private static AtomicLong loadt = new AtomicLong(0);

    private static MongoClient mongoClient = null;

    public static String []ingest;
    public static String database = "VitamLinks";
    public static String host = "localhost";
    public static String esbase = "vitam";
    public static String unicast = "mdb002, mdb003, mdb004";
    public static String model = "courriel";

    /**
     * @param args
     */
    public static void main(final String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("need: logfile host database escluster unicast files");
            return;
        }
        final String networkHost = "192.168.56.102";
        GlobalDatas.localNetworkAddress = networkHost;
        final String log4j = args[0];
        PropertyConfigurator.configure(log4j);
        VitamLoggerFactory.setDefaultFactory(new LogbackLoggerFactory(VitamLogLevel.WARN));
        LOGGER = VitamLoggerFactory.getInstance(MainIngestMDBESFromFile.class);
        // connect to the local database server
        if (args.length > 1) {
            host = args[1];
        }
        if (args.length > 2) {
            database = args[2];
        }
        if (args.length > 3) {
            esbase = args[3];
        }
        if (args.length > 4) {
            unicast = args[4];
        }
        if (args.length > 5) {
            model = args[5];
        }
        if (args.length > 6) {
            ingest = new String[args.length-6];
            for (int i = 0; i < ingest.length; i++) {
                ingest[i] = args[6+i];
            }
        }
        LOGGER.warn("Start with " + ingest + ":" + host + ":" + database + ":" + esbase + ":" + unicast);

        MongoDbAccess dbvitam = null;
        try {
            final MongoClientOptions options = new MongoClientOptions.Builder().connectionsPerHost(10).build();
            mongoClient = new MongoClient(host, options);
            mongoClient.setReadPreference(ReadPreference.primaryPreferred());
            dbvitam = new MongoDbAccess(mongoClient, database, esbase, unicast, true);
            dbvitam.ensureIndex();
            LOGGER.warn(dbvitam.toString());
            
            MainIngestMDBESFromFile.loadt = new AtomicLong(0);
            MainIngestFile.cptMaip.set(0);

            runOnce(dbvitam);

        } catch (final Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();

        } finally {
            // release resources
            final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
            final ToClean toclean = new ToClean(dbvitam);
            scheduler.schedule(toclean, 1, TimeUnit.MILLISECONDS);
            final ToShutdown toShutdown = new ToShutdown();
            scheduler.schedule(toShutdown, 5000, TimeUnit.MILLISECONDS);
            scheduler.awaitTermination(7000, TimeUnit.MILLISECONDS);
            System.exit(0);
        }

    }

    private static final class ToClean implements Runnable {
        MongoDbAccess dbvitam;

        public ToClean(final MongoDbAccess dbvitam) {
            this.dbvitam = dbvitam;
        }

        @Override
        public void run() {
            dbvitam.close();
            mongoClient.close();
        }

    }

    private static final class ToShutdown implements Runnable {

        @Override
        public void run() {
            System.exit(0);
        }

    }

    private static final void runOnce(final MongoDbAccess dbvitam) throws InterruptedException, InstantiationException,
    IllegalAccessException, IOException {
        System.out.println("Load starting... ");
        int nbThread = ingest.length;

        final long date11 = System.currentTimeMillis();
        if (ingest.length == 1) {
            final FileInputStream fstream = new FileInputStream(ingest[0]);
            final DataInputStream in = new DataInputStream(fstream);
            final BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            int nb = 0;
            final HashMap<String, String> esIndex = new HashMap<>();
            BulkWriteOperation bulk = dbvitam.daips.collection.initializeUnorderedBulkOperation();
            while ((strLine = br.readLine()) != null) {
                final DBObject bson = (DBObject) JSON.parse(strLine);
                bulk.insert(bson);
                ElasticSearchAccess.addEsIndex(dbvitam, model, esIndex, bson);
                nb++;
                if (nb % GlobalDatas.LIMIT_MDB_NEW_INDEX == 0) {
                    BulkWriteResult result = bulk.execute();
                    int check = result.getInsertedCount();
                    if (check != nb) {
                        System.out.print("x");
                    } else {
                        System.out.print(".");
                    }
                    bulk = dbvitam.daips.collection.initializeUnorderedBulkOperation();
                    MainIngestFile.cptMaip.addAndGet(check);
                    nb = 0;
                }
            }
            if (!esIndex.isEmpty()) {
                System.out.println("Last bulk ES");
                dbvitam.addEsEntryIndex(true, esIndex, model);
                esIndex.clear();
            }
            if (nb != 0) {
                bulk.execute();
                MainIngestFile.cptMaip.addAndGet(nb);
                nb = 0;
            }
        } else {
            // threads
            ExecutorService executorService = Executors.newFixedThreadPool(ingest.length+1);
            for (int i = 0; i < ingest.length; i++) {
                MainIngestMDBESFromFile ingestrun = new MainIngestMDBESFromFile();
                ingestrun.file = ingest[i];
                executorService.execute(ingestrun);
            }
            // ES
            MainIngestMDBESFromFile ingestrun = new MainIngestMDBESFromFile();
            ingestrun.file = null;
            ingestrun.files = ingest;
            ingestrun.original = dbvitam;
            executorService.execute(ingestrun);
            
            executorService.shutdown();
            while (!executorService.awaitTermination(10000, TimeUnit.MILLISECONDS)) {
                ;
            }
            System.out.println("Load ended");
            final long nbBigM = dbvitam.getDaipSize();
            final long nbBigD = dbvitam.getPaipSize();
            System.out.println("\n Big Test (" + nbThread + " nb MAIP: "
                    + MainIngestFile.cptMaip.get() + ") with MAIP: " + nbBigM + " DATA: " + nbBigD + " => Load:" + (loadt.get())
                    / ((float) MainIngestFile.cptMaip.get()*nbThread));

            System.out.println("\nThread;nbLoad;nbTotal;Load");
            System.out.println(nbThread + ";" + MainIngestFile.cptMaip.get() + ";" + nbBigM + ";" + (loadt.get()) / ((float) MainIngestFile.cptMaip.get()*nbThread));
        }
        final long date12 = System.currentTimeMillis();
        MainIngestMDBESFromFile.loadt.set(date12 - date11);

        System.out.println("Load ended");
        /*
         * System.out.println("All elements\n================================================================");
         * DbVitam.printStructure(dbvitam);
         */
        final long nbBigM = dbvitam.getDaipSize();
        final long nbBigD = dbvitam.getPaipSize();
        System.out.println("\n Big Test (" + nbThread + " Threads chacune " + MainIngestFile.nb
                + " nb MAIP: " + MainIngestFile.cptMaip.get() + ") with MAIP: " + nbBigM + " DATA: " + nbBigD
                + " => Load:" + (MainIngestMDBESFromFile.loadt.get()) / ((float) MainIngestFile.cptMaip.get()));

        System.out.println("\nThread;nbLoad;nbTotal;Load");
        System.out.println(nbThread + ";" + MainIngestFile.cptMaip.get() + ";" + nbBigM + ";"
                + (MainIngestMDBESFromFile.loadt.get()) / ((float) MainIngestFile.cptMaip.get()));
    }

    private String file;
    private String [] files;
    private MongoDbAccess original;
    
    @Override
    public void run() {
        if (file == null) {
            // ES
            //Thread.sleep(1000);
            try {
                for (int i = 0; i < files.length-1; i++) {
                    System.out.println("ESFile: "+files[i]);
                    final HashMap<String, String> esIndex = new HashMap<>();
                    final FileInputStream fstream = new FileInputStream(files[i]);
                    final DataInputStream in = new DataInputStream(fstream);
                    final BufferedReader br = new BufferedReader(new InputStreamReader(in));
                    String strLine;
                    // Read File Line By Line
                    while ((strLine = br.readLine()) != null) {
                        final BSONObject bson = (BSONObject) JSON.parse(strLine);
                        ElasticSearchAccess.addEsIndex(original, model, esIndex, bson);
                    }
                    // Close the input stream
                    br.close();
                    in.close();
                    fstream.close();
                    if (!esIndex.isEmpty()) {
                        System.out.println("Last bulk ES");
                        original.addEsEntryIndex(true, esIndex, model);
                        esIndex.clear();
                    }
                }
                // last file might contains already inserted but to be updated DAip
                int i = files.length-1;
                System.out.println("ESFile: "+files[i]);
                final FileInputStream fstream = new FileInputStream(files[i]);
                final DataInputStream in = new DataInputStream(fstream);
                final BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String strLine;
                // Read File Line By Line
                while ((strLine = br.readLine()) != null) {
                    final BSONObject bson = (BSONObject) JSON.parse(strLine);
                    ElasticSearchAccess.addEsIndex(original, model, bson);
                }
                // Close the input stream
                br.close();
                in.close();
                fstream.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return;
        }
        MongoDbAccess dbvitam = null;
        FileInputStream fstream = null;
        DataInputStream in = null;
        final BufferedReader br;
        try {
            System.out.println("MDFile: "+file);
            fstream = new FileInputStream(file);
            in = new DataInputStream(fstream);
            br = new BufferedReader(new InputStreamReader(in));
            dbvitam = new MongoDbAccess(mongoClient, database, esbase, unicast, false);
            // now ingest metaaip/metafield/data
            final long date11 = System.currentTimeMillis();
            String strLine;
            int nb = 0;
            
            if (false) {
                // Tokumx
                List<DBObject> inserts = new ArrayList<DBObject>(GlobalDatas.LIMIT_MDB_NEW_INDEX);
                while ((strLine = br.readLine()) != null) {
                    final DBObject bson = (DBObject) JSON.parse(strLine);
                    inserts.add(bson);
                    nb++;
                    if (nb % GlobalDatas.LIMIT_MDB_NEW_INDEX == 0) {
                        WriteResult result = dbvitam.daips.collection.insert(inserts);
                        if (result.getN() != nb) {
                            LOGGER.error("Wrong bulk op: "+result);
                        }
                        MainIngestFile.cptMaip.addAndGet(nb);
                        inserts.clear();
                        nb = 0;
                        System.out.print(".");
                    }
                }
                if (nb != 0) {
                    WriteResult result = dbvitam.daips.collection.insert(inserts);
                    if (result.getN() != nb) {
                        LOGGER.error("Wrong bulk op: "+result);
                    }
                    MainIngestFile.cptMaip.addAndGet(nb);
                    inserts.clear();
                    nb = 0;
                }
            } else {
                BulkWriteOperation bulk = dbvitam.daips.collection.initializeUnorderedBulkOperation();
                while ((strLine = br.readLine()) != null) {
                    final DBObject bson = (DBObject) JSON.parse(strLine);
                    bulk.insert(bson);
                    nb++;
                    if (nb % GlobalDatas.LIMIT_MDB_NEW_INDEX == 0) {
                        BulkWriteResult result = bulk.execute();
                        bulk = dbvitam.daips.collection.initializeUnorderedBulkOperation();
                        if (result.getInsertedCount() != nb) {
                            LOGGER.error("Wrong bulk op: "+result);
                        }
                        MainIngestFile.cptMaip.addAndGet(nb);
                        nb = 0;
                        System.out.print(".");
                    }
                }
                if (nb != 0) {
                    BulkWriteResult result = bulk.execute();
                    if (result.getInsertedCount() != nb) {
                        LOGGER.error("Wrong bulk op: "+result);
                    }
                    MainIngestFile.cptMaip.addAndGet(nb);
                    nb = 0;
                }
            }
            final long date12 = System.currentTimeMillis();
            loadt.addAndGet(date12 - date11);
            return;
        } catch (final InvalidUuidOperationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (final FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            // release resources
            try {
                in.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            try {
                fstream.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            if (dbvitam != null) {
                dbvitam.close();
            }
        }
    }
}
