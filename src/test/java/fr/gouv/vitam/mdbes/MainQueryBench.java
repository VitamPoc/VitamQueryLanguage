/**
 * This file is part of POC MongoDB ElasticSearch Project.
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free Software Foundation,
 * either versionRank 3 of the License, or (at your option) any later versionRank.
 * 
 * POC MongoDB ElasticSearch is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with POC MongoDB
 * ElasticSearch . If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.mdbes;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.PropertyConfigurator;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;

import fr.gouv.vitam.query.exception.InvalidExecOperationException;
import fr.gouv.vitam.query.exception.InvalidParseOperationException;
import fr.gouv.vitam.query.exception.InvalidUuidOperationException;
import fr.gouv.vitam.utils.FileUtil;
import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.logging.LogbackLoggerFactory;
import fr.gouv.vitam.utils.logging.VitamLogLevel;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * @author "Frederic Bregier"
 * 
 */
public class MainQueryBench implements Runnable {
    private static VitamLogger LOGGER = null;
	private static AtomicLong depthmax = new AtomicLong();
	private static AtomicLong tree = new AtomicLong();

	private static MongoClient mongoClient = null;
	private static final int MAXTHREAD = 8;
	/**
	 * Either 0, either x*2*10
	 */
	private static final int waitBetweenQuery = 20;
	
    private static boolean simulate = false;
    private static String fileDepth;
    private static String fileTree;
    private static String host = "localhost";
    private static String database = "VitamLinks";
    private static String esbase = "vitam";
    private static String unicast = "mdb002, mdb003, mdb004";
    private static int repeatNb = 100;
    private static int nbload = 1;
    private static int nbThread = 1;
    protected static AtomicLong cptMaip = new AtomicLong();

    private int start, stop;
	

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 7) {
			System.err.println("need: logfile host database esbase unicast nbload filequeryDepth filequeryTree");
			return;
		}
        final String log4j = args[0];
        PropertyConfigurator.configure(log4j);
        VitamLoggerFactory.setDefaultFactory(new LogbackLoggerFactory(VitamLogLevel.WARN));
        LOGGER = VitamLoggerFactory.getInstance(MainQueryBench.class);
        final String networkHost = "192.168.56.102";
        GlobalDatas.localNetworkAddress = networkHost;
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
        nbload = 1;
        if (args.length > 5) {
            nbload = Integer.parseInt(args[5]);
        }
        if (args.length > 6) {
            fileDepth = FileUtil.readFile(args[6]);
        }
        if (args.length > 7) {
            fileTree = FileUtil.readFile(args[7]);
        }
		// connect to the local database server
        MongoDbAccess dbvitam = null;
        try {
            final MongoClientOptions options = new MongoClientOptions.Builder().connectionsPerHost(MAXTHREAD).build();
            mongoClient = new MongoClient(host, options);
            mongoClient.setReadPreference(ReadPreference.primaryPreferred());
            dbvitam = new MongoDbAccess(mongoClient, database, esbase, unicast, false);
			int nbt = 1;
			for (nbt = 1; nbt <= MAXTHREAD; nbt *= 2) {
			//for (nbt = 1; nbt <= maxThread; nbt += 2) {
				// for (; nbt <= maxThread; nbt += 100) {
				nbThread = nbt;
				MainQueryBench.tree = new AtomicLong();
				MainQueryBench.depthmax = new AtomicLong();
				cptMaip.set(0);
				runOnce(dbvitam);
			}
        } catch (Exception e) {
            LOGGER.error(e);
		} finally {
			// release resources
			dbvitam.close();
			mongoClient.close();
		}
	}

	protected static void runOnce(MongoDbAccess dbvitam) throws InterruptedException,
			InstantiationException, IllegalAccessException, IOException {
		MainQueryBench[] ingests = new MainQueryBench[nbThread];
		//GlobalDatas.nb = GlobalDatas.nb / GlobalDatas.nbThread;
		ExecutorService executorService = null;
		LOGGER.warn("Unitary test\n================================================================================================================================");
		try {
			oneShot(dbvitam);
		} catch (InvalidParseOperationException | InvalidExecOperationException e) {
            LOGGER.error(e);
			return;
		}
		Thread.sleep(2000);
		LOGGER.warn("requests\n================================================================================================================================");
		executorService = Executors.newFixedThreadPool(nbThread);
		//int step = 0;
		int interval = nbload;
		for (int i = 0; i < nbThread; i++) {
			ingests[i] = new MainQueryBench();
			ingests[i].start = 0;
			ingests[i].stop = interval - 1;
			//ingests[i].start = step;
			//ingests[i].stop = step + interval - 1;
			//step += interval;
			executorService.execute(ingests[i]);
		}
		Thread.sleep(1000);
		executorService.shutdown();
		while (!executorService.awaitTermination(10000, TimeUnit.MILLISECONDS)) {
			;
		}
		long nbBigM = dbvitam.getDaipSize();
		LOGGER.warn("Big Test (" + nbThread + " Threads each with "+nbload*repeatNb+" requests) with MAIP: " + nbBigM
				+ " Tree:"
				+ (MainQueryBench.tree.get()) / ((float) nbload * repeatNb * nbThread)
				+ " DepthMax:" + (MainQueryBench.depthmax.get())
				/ ((float) nbload * repeatNb * nbThread));

		LOGGER.warn("Thread;nbReq;nbMaip;Tree;DepthMax");
		LOGGER.warn(nbThread + ";" + (nbload * repeatNb * nbThread) + ";" + nbBigM
				+ ";" 
				+ (MainQueryBench.tree.get()) / ((float) nbload * repeatNb * nbThread)
				+ ";"
				+ (MainQueryBench.depthmax.get())
				/ ((float) nbload * repeatNb * nbThread)+"\n");
	}

	protected static void oneShot(MongoDbAccess dbvitam) throws InvalidParseOperationException, InvalidExecOperationException, InstantiationException, IllegalAccessException {
		String comdtree = fileTree.toString();
		ParserBench commandTree = new ParserBench(simulate);
		commandTree.prepareParse(comdtree);
        BenchContext contextTree = commandTree.getNewContext(GlobalDatas.INDEXNAME, commandTree.model);
        String comddepth = fileDepth.toString();
		ParserBench commandDepth = new ParserBench(simulate);
		commandDepth.prepareParse(comddepth);
		BenchContext contextDepth = commandDepth.getNewContext(GlobalDatas.INDEXNAME, commandDepth.model);
		// Requesting
		long date13 = System.currentTimeMillis();
		List<ResultCached> results = commandTree.executeBenchmark(dbvitam, 0, contextTree);
		long date14 = System.currentTimeMillis();
		ResultCached result = commandTree.finalizeResults(dbvitam, contextTree, results);
		long date15 = System.currentTimeMillis();

        long date23 = System.currentTimeMillis();
        List<ResultCached> results2 = commandDepth.executeBenchmark(dbvitam, 0, contextDepth);
        long date24 = System.currentTimeMillis();
        ResultCached result2 = commandDepth.finalizeResults(dbvitam, contextDepth, results2);
        long date25 = System.currentTimeMillis();

		long nbUnitaryM = dbvitam.getDaipSize();
		LOGGER.warn("Unitary Test Tree with DAIP: " + nbUnitaryM 
				+ " => TreeExec:" + (date14 - date13) +
				" TreePath:" + (date15 - date14));
		if (result != null && ! result.currentDaip.isEmpty()) {
            LOGGER.warn("ResTree= "+result.currentDaip.size());
		    /*for (String id : result.currentDaip) {
	            DAip daip = DAip.findOne(dbvitam, UUID.getLastAsString(id));
                LOGGER.warn("ResTree: "+daip);
            }*/
		} else {
		    LOGGER.error("ResTree : no result");
		}
        LOGGER.warn("Unitary Test Depth with DAIP: " + nbUnitaryM 
                + " => DepthExec:" + (date24 - date23) +
                " DepthPath:" + (date25 - date24));
        if (result2 != null && ! result2.currentDaip.isEmpty()) {
            LOGGER.warn("ResDepth= "+result2.currentDaip.size());
            /*for (String id : result2.currentDaip) {
                DAip daip = DAip.findOne(dbvitam, UUID.getLastAsString(id));
                LOGGER.warn("\tResDepth: "+daip);
            }*/
        } else {
            LOGGER.error("ResDepth : no result");
        }
	}

	@Override
	public void run() {
		// connect to the local database server
        String comdtree = fileTree.toString();
        String comddepth = fileDepth.toString();
		MongoDbAccess dbvitam = null;
		try {
		    dbvitam = new MongoDbAccess(mongoClient, database, esbase, unicast, false);
	        ParserBench commandTree = new ParserBench(simulate);
	        commandTree.prepareParse(comdtree);
	        BenchContext contextTree = commandTree.getNewContext(GlobalDatas.INDEXNAME, commandTree.model);
	        ParserBench commandDepth = new ParserBench(simulate);
	        commandDepth.prepareParse(comddepth);
	        BenchContext contextDepth = commandDepth.getNewContext(GlobalDatas.INDEXNAME, commandDepth.model);
			// Tree
			ThreadLocalRandom random = ThreadLocalRandom.current();
			long[] extraTimes = { 0, 0 };
			long minTime = waitBetweenQuery / 2;
			long date1 = System.currentTimeMillis();
			for (int repeat = 0; repeat < repeatNb; repeat++) {
				for (int i = start; i <= stop; i++) {
			        List<ResultCached> results = commandTree.executeBenchmark(dbvitam, i, contextTree);
			        ResultCached result = commandTree.finalizeResults(dbvitam, contextTree, results);
					if (waitBetweenQuery > 0) {
						long time = ( random.nextLong(minTime, waitBetweenQuery) / 10 )* 10;
						extraTimes[0] += time;
						try {
							Thread.sleep(time);
						} catch (InterruptedException e) {
						}
					}
				}
			}
			long date2 = System.currentTimeMillis();
			for (int repeat = 0; repeat < repeatNb; repeat++) {
				for (int i = start; i <= stop; i++) {
                    List<ResultCached> results = commandDepth.executeBenchmark(dbvitam, i, contextDepth);
                    ResultCached result = commandDepth.finalizeResults(dbvitam, contextDepth, results);
					if (waitBetweenQuery > 0) {
						long time = ( random.nextLong(minTime, waitBetweenQuery) / 10 )* 10;
						extraTimes[1] += time;
						try {
							Thread.sleep(time);
						} catch (InterruptedException e) {
						}
					}
				}
			}
			long date3 = System.currentTimeMillis();
			MainQueryBench.tree.addAndGet(date2 - date1 - extraTimes[0]);
			MainQueryBench.depthmax.addAndGet(date3 - date2 - extraTimes[1]);
		} catch (InvalidExecOperationException e1) {
			LOGGER.error(e1);
		} catch (InvalidParseOperationException e1) {
            LOGGER.error(e1);
		} catch (InvalidUuidOperationException e1) {
            LOGGER.error(e1);
        } catch (InstantiationException e1) {
            LOGGER.error(e1);
        } catch (IllegalAccessException e1) {
            LOGGER.error(e1);
        } finally {
			// release resources
            if (dbvitam != null) {
                dbvitam.close();
            }
		}
	}

}
