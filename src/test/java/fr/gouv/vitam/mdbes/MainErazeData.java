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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.PropertyConfigurator;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;

import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.logging.LogbackLoggerFactory;
import fr.gouv.vitam.utils.logging.VitamLogLevel;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * ErazeData
 *
 * @author "Frederic Bregier"
 *
 */
@SuppressWarnings("javadoc")
public class MainErazeData {
    private static VitamLogger LOGGER = null;

    private static MongoClient mongoClient = null;
    private static int MAXTHREAD = 1;

    private static boolean eraze = false;
    private static boolean simulate = false;
    private static String model;
    private static String host = "localhost";
    private static String database = "VitamLinks";
    private static String esbase = "vitam";
    private static String unicast = "mdb002, mdb003, mdb004";

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
     *            <li>eraze/noeraze = eraze will delete all data in DB (!), else nothing is done</li>
     *            <li>host = MongoDB host</li>
     *            <li>database = MongoDB database name as VitamLinks</li>
     *            <li>escluster = ElasticSearch cluster name</li>
     *            <li>unicast = ElasticSearch unicast servers list (as in "mdb001, mdb002, mdb003")</li>
     *            </ol>
     */
    public static void main(final String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("need: logfile eraze/noeraze host database escluster unicast");
            return;
        }
        final String log4j = args[0];
        PropertyConfigurator.configure(log4j);
        VitamLoggerFactory.setDefaultFactory(new LogbackLoggerFactory(VitamLogLevel.WARN));
        LOGGER = VitamLoggerFactory.getInstance(MainErazeData.class);
        MongoDbAccess dbvitam = null;
        try {
            if (args.length > 1) {
                eraze = args[1].equals("eraze");
            }
            if (simulate) {
                eraze = false;
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
            LOGGER.debug("Start with " + eraze + ":" + host + ":" + database + ":" + esbase + ":" + unicast);
            MAXTHREAD += nbThread;
            final MongoClientOptions options = new MongoClientOptions.Builder().connectionsPerHost(MAXTHREAD).build();
            mongoClient = new MongoClient(host, options);
            mongoClient.setReadPreference(ReadPreference.primaryPreferred());
            dbvitam = new MongoDbAccess(mongoClient, database, esbase, unicast, eraze);
            // get a list of the collections in this database and print them out
            LOGGER.debug(dbvitam.toString());
            if (eraze) {
                reinit(dbvitam);
                LOGGER.warn(dbvitam.toString());
                return;
            }
            return;
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

    private static final class ToClean implements Runnable {
        MongoDbAccess dbvitam;

        private ToClean(final MongoDbAccess dbvitam) {
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

    private static final void reinit(final MongoDbAccess dbvitam) throws InterruptedException {
        GlobalDatas.ROOTS.clear();
        // GlobalDatas.maxDepth.clear();
        dbvitam.reset(model);
        Thread.sleep(1000);
        // Initialize default DuaRefs
        final DuaRef ref25 = new DuaRef("25ans", 25 * 12);
        final DuaRef ref50 = new DuaRef("50ans", 50 * 12);
        final DuaRef ref100 = new DuaRef("100ans", 100 * 12);
        final DuaRef refDefinitif = new DuaRef("Definitif", -1);
        ref25.save(dbvitam);
        ref50.save(dbvitam);
        ref100.save(dbvitam);
        refDefinitif.save(dbvitam);
    }
}
