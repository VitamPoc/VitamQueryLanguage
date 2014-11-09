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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.PropertyConfigurator;
import org.bson.BSONObject;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.util.JSON;

import fr.gouv.vitam.query.GlobalDatas;
import fr.gouv.vitam.query.exception.InvalidExecOperationException;
import fr.gouv.vitam.utils.FileUtil;
import fr.gouv.vitam.utils.exception.InvalidParseOperationException;
import fr.gouv.vitam.utils.exception.InvalidUuidOperationException;
import fr.gouv.vitam.utils.logging.LogbackLoggerFactory;
import fr.gouv.vitam.utils.logging.VitamLogLevel;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * Ingest file into ElasticSearch and into file, then eventually in MongoDB
 *
 * @author "Frederic Bregier"
 *
 */
@SuppressWarnings("javadoc")
public class MainIngestFile implements Runnable {
    private static VitamLogger LOGGER = null;

    protected static AtomicLong loadt = new AtomicLong(0);
    protected static AtomicLong mongoLoad = new AtomicLong(0);

    protected static MongoClient mongoClient = null;
    protected static int MAXTHREAD = 1;

    protected static boolean simulate = false;
    protected static String ingest;
    protected static String model;
    protected static int startFrom = 0;
    protected static String host = "localhost";
    protected static String database = "VitamLinks";
    protected static String esbase = "vitam";
    protected static String unicast = "mdb002, mdb003, mdb004";
    protected static String commandMongo = null;
    protected static String fileout = null;
    protected static List<File> files = new ArrayList<File>();

    protected ParserIngest parserIngest;
    protected int start, stop;

    protected static int nb = 400;
    protected static int firstLevel = 10;// was 100
    protected static int lastLevel = 1000; // was 1000
    protected static float nbr = 100; // could enhance the number of request with 1000
    protected static long waitBetweenQuery = 200; // could be used to simulate Little's law, for instance = 100ms
    protected static int minleveltofile = 0;
    protected static AtomicLong cptMaip = new AtomicLong();
    protected static int nbThread = 1;

    /**
     * Will save to a File and to ElasticSearch, then to MongoDB
     *
     * @param args
     *            logfile eraze/noeraze host database escluster unicast start nbload file fileout limitdepth nbThread mongoimport
     *            stopindex/xx
     *
     *            <ol>
     *            <li>logfile = Log4J configuration log file</li>
     *            <li>noeraze/index = index will (re)create index, else nothing is done</li>
     *            <li>host = MongoDB host</li>
     *            <li>database = MongoDB database name as VitamLinks</li>
     *            <li>escluster = ElasticSearch cluster name</li>
     *            <li>unicast = ElasticSearch unicast servers list (as in "mdb001, mdb002, mdb003")</li>
     *            <li>start = start index in the bench (will be for instance between 1-1000 start from 100)</li>
     *            <li>nbload = number of iteration from start</li>
     *            <li>file = ingest file</li>
     *            <li>fileout = output saved</li>
     *            <li>limitdepth = from which level the output is saved to the file and not to MongoDB</li>
     *            <li>nbThread = number of thread (default 1)</li>
     *            <li>mongoimport = optional command for import</li>
     *            <li>stopindex/xx = shall we stop index during import in MongoDB</li>
     *            </ol>
     */
    public static void main(final String[] args) throws Exception {
        if (args.length < 6) {
            System.err
            .println("need: logfile noeraze/index host database escluster unicast start nbload file fileout limitdepth mongoimport stopindex/xx nbThread");
            // System.err.println("before was need: logfile nbload files eraze/noeraze start host escluster unicast fileout limitdepth mongoimport 0/1 (1=stop index)");
            return;
        }
        final String log4j = args[0];
        PropertyConfigurator.configure(log4j);
        VitamLoggerFactory.setDefaultFactory(new LogbackLoggerFactory(VitamLogLevel.WARN));
        LOGGER = VitamLoggerFactory.getInstance(MainIngestFile.class);
        boolean reindex = false;
        if (args.length > 1) {
            reindex = args[1].equals("index");
        }
        if (simulate) {
            reindex = false;
        }
        final String networkHost = "192.168.56.102";
        GlobalDatas.localNetworkAddress = networkHost;
        // connect to the local database server
        if (args.length > 2) {
            host = args[2];
        }
        if (args.length > 3) {
            database = args[3];
        }
        if (args.length > 4) {
            esbase = args[4];
        }
        if (args.length > 5) {
            unicast = args[5];
        }
        int realnb = -1;
        if (args.length > 6) {
            startFrom = Integer.parseInt(args[6]);
        }
        if (args.length > 7) {
            realnb = Integer.parseInt(args[7]);
        }
        if (args.length > 8) {
            ingest = FileUtil.readFile(args[8]);
        }
        if (args.length > 9) {
            fileout = args[9];
        }
        if (args.length > 10) {
            final int stoplevel = Integer.parseInt(args[10]);
            minleveltofile = stoplevel;
        }
        if (args.length > 11) {
            nbThread = Integer.parseInt(args[11]);
        }
        if (args.length > 12) {
            commandMongo = args[12];
        }
        boolean stopindex = false;
        if (args.length > 13) {
            stopindex = args[13].equalsIgnoreCase("stopindex");
        }
        LOGGER.debug("Start with " + reindex + ":" + host + ":" + database + ":" + esbase + ":" + unicast);
        if (args.length > 6) {
            LOGGER.debug("and " + startFrom + ":" + realnb + ":" + ingest + ":" + fileout + ":" + minleveltofile + ":" + nbThread
                    + ":" + commandMongo + ":" + stopindex);
        }
        MongoDbAccess dbvitam = null;
        try {
            MAXTHREAD += nbThread;
            final MongoClientOptions options = new MongoClientOptions.Builder().connectionsPerHost(MAXTHREAD).build();
            mongoClient = new MongoClient(host, options);
            mongoClient.setReadPreference(ReadPreference.primaryPreferred());
            dbvitam = new MongoDbAccess(mongoClient, database, esbase, unicast, reindex);
            // get a list of the collections in this database and print them out
            LOGGER.debug(dbvitam.toString());
            if (realnb < 0) {
                return;
            }
            // drop all the data in it
            final ParserIngest parser = new ParserIngest(true);
            parser.parse(ingest);
            model = parser.getModel();
            if (reindex) {
                LOGGER.debug("ensureIndex");
                dbvitam.ensureIndex();
                if (model != null) {
                    LOGGER.debug("updateEsIndex");
                    dbvitam.updateEsIndex(model);
                }
                LOGGER.debug("end Index");
            }
            LOGGER.warn(dbvitam.toString());

            final int stepnb = realnb;
            nb = stepnb;
            loadt = new AtomicLong(0);
            cptMaip.set(0);
            runOnce(dbvitam);
            // Continue with MongoDB Loading if setup
            if (commandMongo != null) {
                System.out.println("Launch MongoImport");
                runOnceMongo(dbvitam, stopindex);
            }
        } catch (Exception e) {
            LOGGER.error(e);
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

    protected static final class ToClean implements Runnable {
        MongoDbAccess dbvitam;

        protected ToClean(final MongoDbAccess dbvitam) {
            this.dbvitam = dbvitam;
        }

        @Override
        public void run() {
            dbvitam.close();
            mongoClient.close();
        }

    }

    protected static final class ToShutdown implements Runnable {

        @Override
        public void run() {
            System.exit(0);
        }

    }

    private static final void runOnce(final MongoDbAccess dbvitam) throws InterruptedException, InstantiationException,
    IllegalAccessException, IOException {
        MainIngestFile ingests = null;
        nb = nb / nbThread;
        int step = startFrom;
        final int interval = nb;
        LOGGER.warn("Load starting... " + nbThread + ":" + nb + ":" + interval);
        for (int i = 0; i < nbThread; i++) {
            ingests = new MainIngestFile();
            ingests.start = step;
            ingests.stop = step + interval - 1;
            step += interval;
            ingests.run();
        }
        System.out.println("Load ended");
        final FileOutputStream outputStream = new FileOutputStream(fileout);
        final Map<String, String> esIndex = new HashMap<String, String>();
        for (DAip daip : ParserIngest.savedDaips.values()) {
            daip.load(dbvitam);
            daip.toFile(outputStream);
            final BSONObject bson = (BSONObject) JSON.parse(daip.toStringDirect());
            ElasticSearchAccess.addEsIndex(dbvitam, model, esIndex, bson);
        }
        outputStream.close();
        if (!esIndex.isEmpty()) {
            MainIngestFile.cptMaip.addAndGet(esIndex.size());
            System.out.println("Last bulk ES");
            dbvitam.addEsEntryIndex(true, esIndex, model);
            esIndex.clear();
        }

        /*
         * System.out.println("All elements\n================================================================");
         * DbVitam.printStructure(dbvitam);
         */
        final long nbBigM = dbvitam.getDaipSize();
        final long nbBigD = dbvitam.getPaipSize();
        System.out.println("\n Big Test (" + nbThread + " Threads chacune " + nb + " itérations de load, nb MAIP: "
                + cptMaip.get() + ") with MAIP: " + nbBigM + " DATA: " + nbBigD + " => Load:" + (loadt.get())
                / ((float) cptMaip.get()));

        System.out.println("\nThread;nbLoad;nbTotal;Load");
        System.out.println(nbThread + ";" + cptMaip.get() + ";" + nbBigM + ";" + (loadt.get()) / ((float) cptMaip.get()));
    }

    @Override
    public void run() {
        MongoDbAccess dbvitam = null;
        try {
            dbvitam = new MongoDbAccess(mongoClient, database, esbase, unicast, false);
            // now ingest metaaip/metafield/data
            final long date11 = System.currentTimeMillis();
            parserIngest = new ParserIngest(simulate);

            final String filename = fileout+"-"+start+"-"+stop+".json";
            final File file = new File(filename);
            final FileOutputStream outputStream = new FileOutputStream(file);
            parserIngest.bufferedOutputStream = new BufferedOutputStream(outputStream);
            System.out.println("Start to File: "+filename);
            parserIngest.parse(ingest);
            parserIngest.executeToFile(dbvitam, start, stop, true);
            final long date12 = System.currentTimeMillis();
            loadt.addAndGet(date12 - date11);
            cptMaip.addAndGet(parserIngest.getTotalCount());
            files.add(file);
            return;
        } catch (final InvalidExecOperationException e) {
            LOGGER.error(e);
        } catch (final InvalidParseOperationException e) {
            LOGGER.error(e);
        } catch (final InvalidUuidOperationException e) {
            LOGGER.error(e);
        } catch (final FileNotFoundException e) {
            LOGGER.error(e);
        } finally {
            // release resources
            if (parserIngest.bufferedOutputStream != null) {
                if (GlobalDatas.PRINT_REQUEST) {
                    System.out.println("Flushing file");
                }
                try {
                    parserIngest.bufferedOutputStream.flush();
                } catch (final IOException e) {
                }
                if (GlobalDatas.PRINT_REQUEST) {
                    System.out.println("Closing file");
                }
                try {
                    parserIngest.bufferedOutputStream.close();
                } catch (final IOException e) {
                }
                if (GlobalDatas.PRINT_REQUEST) {
                    System.out.println("File closed");
                }
            }
            if (dbvitam != null) {
                dbvitam.close();
            }
        }
    }

    private static final void runOnceMongo(final MongoDbAccess dbvitam, final boolean stopindex) throws InterruptedException,
    InstantiationException, IllegalAccessException, IOException {
        if (stopindex) {
            dbvitam.removeIndexBeforeImport();
        }
        final MongoRunImport[] ingests = new MongoRunImport[nbThread];
        ExecutorService executorService = null;
        executorService = Executors.newFixedThreadPool(nbThread);
        System.out.print("Load starting... " + nbThread + ":" + nb);
        for (int i = 1; i < nbThread; i++) {
            ingests[i] = new MongoRunImport();
            ingests[i].file = files.get(i);
            executorService.execute(ingests[i]);
        }
        Thread.sleep(1000);
        executorService.shutdown();
        while (!executorService.awaitTermination(10000, TimeUnit.MILLISECONDS)) {
            ;
        }
        System.out.println("Load ended");
        final long start = System.currentTimeMillis();
        if (stopindex) {
            dbvitam.resetIndexAfterImport();
        }
        final long stop = System.currentTimeMillis();
        System.out.println("End of MongoImport MaipES:MaipMD:MaipReindex:MaipTotal:MAIP/s");
        System.out.println("End of MongoImport " + (loadt.get()) / ((float) cptMaip.get()) + ":" + (mongoLoad.get())
                / ((float) cptMaip.get()) + ":" + (stop - start) / ((float) cptMaip.get()) + ":"
                + (loadt.get() + mongoLoad.get() + stop - start) / ((float) cptMaip.get()) + ":"
                + (cptMaip.get() / ((double) (loadt.get() + mongoLoad.get() + stop - start))));
    }

    private static class MongoRunImport implements Runnable {
        private File file;

        @Override
        public void run() {
            final long date11 = System.currentTimeMillis();
            System.out.println("Launch MongoImport");
            final Runtime runtime = Runtime.getRuntime();
            final String cmd = commandMongo + " -h " + host + " -d VitamLinks -c DAip --upsert --file " + file.getAbsolutePath();
            System.out.println("after removing index, start cmd: " + cmd);
            final Process process;
            try {
                process = runtime.exec(cmd);
            } catch (final IOException e1) {
                // TODO Auto-generated catch block
                LOGGER.error(e1);
                return;
            }
            // Consommation de la sortie standard de l'application externe dans un Thread separe
            new Thread() {
                @Override
                public void run() {
                    try {
                        final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                        String line = "";
                        try {
                            while ((line = reader.readLine()) != null) {
                                System.out.println(line);
                                // Traitement du flux de sortie de l'application si besoin est
                            }
                        } finally {
                            reader.close();
                        }
                    } catch (final IOException ioe) {
                        LOGGER.error(ioe);
                    }
                }
            }.start();

            // Consommation de la sortie d'erreur de l'application externe dans un Thread separe
            new Thread() {
                @Override
                public void run() {
                    try {
                        final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                        String line = "";
                        try {
                            while ((line = reader.readLine()) != null) {
                                System.err.println(line);
                                // Traitement du flux d'erreur de l'application si besoin est
                            }
                        } finally {
                            reader.close();
                        }
                    } catch (final IOException ioe) {
                        LOGGER.error(ioe);
                    }
                }
            }.start();
            try {
                process.waitFor();
            } catch (final InterruptedException e) {
                LOGGER.error(e);
            }
            final long date12 = System.currentTimeMillis();
            mongoLoad.addAndGet(date12 - date11);
        }
    }
}
