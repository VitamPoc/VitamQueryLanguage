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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.PropertyConfigurator;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;

import fr.gouv.vitam.mdbes.DuaRef;
import fr.gouv.vitam.mdbes.MongoDbAccess;
import fr.gouv.vitam.query.exception.InvalidExecOperationException;
import fr.gouv.vitam.query.exception.InvalidParseOperationException;
import fr.gouv.vitam.query.exception.InvalidUuidOperationException;
import fr.gouv.vitam.utils.FileUtil;
import fr.gouv.vitam.utils.GlobalDatas;

/**
 * @author "Frederic Bregier"
 * 
 */
public class CopyOfMainIngestFile implements Runnable {
	private static AtomicLong loadt = new AtomicLong(0);
	
	private static MongoClient mongoClient = null;
	private static final int MAXTHREAD = 1;
	
	private static boolean eraze = true;
	private static boolean simulate = false;
	private static String ingest;
	private static String model;
	private static int startFrom = 0;
	private static String host = "localhost";
	private static String esbase = "vitam";
	private static String unicast = "mdb002, mdb003, mdb004";
	private static String commandMongo = null;
	private static String fileout = null;
	
	private ParserIngest parserIngest;
	private int start, stop;
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 8) {
			System.err.println("need: logfile eraze/noeraze host escluster unicast nbload files start fileout limitdepth mongoimport 0/1");
			System.err.println("need: logfile nbload files eraze/noeraze start host escluster unicast fileout limitdepth mongoimport 0/1");
			return;
		}
		String log4j = args[0];
		PropertyConfigurator.configure(log4j);
		if (args.length > 1) {
			eraze = args[1].equals("eraze");
		}
		// connect to the local database server
		if (args.length > 2) {
			host = args[2];
		}
		if (args.length > 3) {
			esbase = args[3];
		}
		if (args.length > 4) {
			unicast = args[4];
		}
		int realnb = -1;
		if (args.length > 5) {
			realnb = Integer.parseInt(args[5]);
		}
		if (args.length > 6) {
			ingest = FileUtil.readFile(args[6]);
		}
		if (args.length > 7) {
			startFrom = Integer.parseInt(args[7]);
		}
		if (simulate) {
			eraze = false;
		}
		if (args.length > 8) {
			fileout = args[8];
			File file = new File(fileout);
			FileOutputStream outputStream = new FileOutputStream(file);
			ParserIngest.bufferedOutputStream = new BufferedOutputStream(outputStream);
		}
		if (args.length > 9) {
			int stoplevel = Integer.parseInt(args[9]);
			GlobalDatas.minleveltofile = stoplevel;
		}
		if (args.length > 10) {
			commandMongo = args[10];
		}
		boolean stopindex = false;
		if (args.length > 11) {
			stopindex = args[11].equals("1");
		}
		MongoDbAccess dbvitam = null;
		try {
			MongoClientOptions options = new MongoClientOptions.Builder().connectionsPerHost(MAXTHREAD).build();
			mongoClient = new MongoClient(host, options);
			mongoClient.setReadPreference(ReadPreference.primaryPreferred());
			boolean reindex = eraze;
			dbvitam = new MongoDbAccess(mongoClient, "VitamLinks", esbase, unicast, reindex);
			// get a list of the collections in this database and print them out
			System.out.println(dbvitam.toString());
			if (CopyOfMainIngestFile.eraze) {
				reinit(dbvitam);
				return;
			}
			dbvitam.updateEsIndex(model);
			// drop all the data in it
			ParserIngest parser = new ParserIngest(true);
			parser.parse(ingest);
			model = parser.getModel();
			
			int stepnb = realnb;
			GlobalDatas.nb = stepnb;
			GlobalDatas.nbThread = 1;
			CopyOfMainIngestFile.loadt = new AtomicLong(0);
			GlobalDatas.cptMaip.set(0);
			if (stopindex) {
				dbvitam.removeIndexBeforeImport();
			}
			runOnce(dbvitam);
		} finally {
			// release resources
			ScheduledExecutorService scheduler =
				     Executors.newScheduledThreadPool(2);
			if (! CopyOfMainIngestFile.eraze) {
				System.out.println("Flushing file");
				ParserIngest.bufferedOutputStream.flush();
				System.out.println("Closing file");
				ParserIngest.bufferedOutputStream.close();
				System.out.println("File closed");
				if (commandMongo != null) {
					System.out.println("Launch MongoImport");
					long start = System.currentTimeMillis();
					Runtime runtime = Runtime.getRuntime();
					String cmd = commandMongo+" -h "+host+" -d VitamLinks -c DAip --upsert --file "+fileout;
					System.out.println("after removing index, start cmd: "+cmd);
					final Process process = runtime.exec(cmd);
					// Consommation de la sortie standard de l'application externe dans un Thread separe
					new Thread() {
						public void run() {
							try {
								BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
								String line = "";
								try {
									while((line = reader.readLine()) != null) {
										System.out.println(line);
										// Traitement du flux de sortie de l'application si besoin est
									}
								} finally {
									reader.close();
								}
							} catch(IOException ioe) {
								ioe.printStackTrace();
							}
						}
					}.start();
	
					// Consommation de la sortie d'erreur de l'application externe dans un Thread separe
					new Thread() {
						public void run() {
							try {
								BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
								String line = "";
								try {
									while((line = reader.readLine()) != null) {
										System.err.println(line);
										// Traitement du flux d'erreur de l'application si besoin est
									}
								} finally {
									reader.close();
								}
							} catch(IOException ioe) {
								ioe.printStackTrace();
							}
						}
					}.start();
					process.waitFor();
					long importdone = System.currentTimeMillis();
					if (stopindex) {
						dbvitam.resetIndexAfterImport();
					}
					long stop = System.currentTimeMillis();
					System.out.println("End of MongoImport MaipES:MaipMD:MaipReindex:MaipTotal:MAIP/s");
					System.out.println("End of MongoImport "+
							(CopyOfMainIngestFile.loadt.get()) / ((float) GlobalDatas.cptMaip.get())+":"+
							(importdone-start) / ((float) GlobalDatas.cptMaip.get())+":"+
							(stop-importdone) / ((float) GlobalDatas.cptMaip.get())+":"+
							(CopyOfMainIngestFile.loadt.get()+stop-start) / ((float) GlobalDatas.cptMaip.get())+
							":"+ (GlobalDatas.cptMaip.get() / ((double)(CopyOfMainIngestFile.loadt.get()+stop-start))));
				}
			}
			ToClean toclean = new ToClean(dbvitam);
			scheduler.schedule(toclean, 1, TimeUnit.MILLISECONDS);
			ToShutdown toShutdown = new ToShutdown();
			scheduler.schedule(toShutdown, 5000, TimeUnit.MILLISECONDS);
			scheduler.awaitTermination(7000, TimeUnit.MILLISECONDS);
			System.exit(0);
		}
			
	}

	private static final class ToClean implements Runnable {
		MongoDbAccess dbvitam;
		private ToClean(MongoDbAccess dbvitam) {
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
	
	private static final void reinit(MongoDbAccess dbvitam) throws InterruptedException {
		GlobalDatas.ROOTS.clear();
		//GlobalDatas.maxDepth.clear();
		dbvitam.reset(model);
		Thread.sleep(1000);
		// Initialize default DuaRefs
		DuaRef ref25 = new DuaRef("25ans", 25 * 12);
		DuaRef ref50 = new DuaRef("50ans", 50 * 12);
		DuaRef ref100 = new DuaRef("100ans", 100 * 12);
		DuaRef refDefinitif = new DuaRef("Definitif", -1);
		ref25.save(dbvitam);
		ref50.save(dbvitam);
		ref100.save(dbvitam);
		refDefinitif.save(dbvitam);
	}
	
	private static final void runOnce(MongoDbAccess dbvitam) throws InterruptedException, InstantiationException, IllegalAccessException, IOException {
		CopyOfMainIngestFile[] ingests = new CopyOfMainIngestFile[GlobalDatas.nbThread];
		GlobalDatas.nb = GlobalDatas.nb / GlobalDatas.nbThread;
		ExecutorService executorService = null;
		int step = startFrom;
		executorService = Executors.newFixedThreadPool(GlobalDatas.nbThread);
		int interval = GlobalDatas.nb;
		System.out.print("Load starting... "+GlobalDatas.nbThread+":"+GlobalDatas.nb+":"+interval);
		ingests[0] = new CopyOfMainIngestFile();
		ingests[0].start = step;
		ingests[0].stop = step + interval - 1;
		step += interval;
		executorService.execute(ingests[0]);
		Thread.sleep(100);
		for (int i = 1; i < GlobalDatas.nbThread; i++) {
			ingests[i] = new CopyOfMainIngestFile();
			ingests[i].start = step;
			ingests[i].stop = step + interval - 1;
			step += interval;
			executorService.execute(ingests[i]);
		}
		Thread.sleep(1000);
		executorService.shutdown();
		while (!executorService.awaitTermination(10000, TimeUnit.MILLISECONDS)) {
			;
		}
		System.out.println("Load ended");
		/*System.out.println("All elements\n================================================================");
		DbVitam.printStructure(dbvitam);*/
		long nbBigM = dbvitam.getDaipSize();
		long nbBigD = dbvitam.getPaipSize();
		System.out.println("\n Big Test ("+GlobalDatas.nbThread + " Threads chacune " + GlobalDatas.nb+" itérations de load, nb MAIP: "+GlobalDatas.cptMaip.get()+") with MAIP: " + nbBigM + " DATA: " + nbBigD
				+ " => Load:" + (CopyOfMainIngestFile.loadt.get()) / ((float) GlobalDatas.cptMaip.get()));
		
		System.out.println("\nThread;nbLoad;nbTotal;Load");
		System.out.println(GlobalDatas.nbThread+";"+GlobalDatas.cptMaip.get()+";"+nbBigM+";"+(CopyOfMainIngestFile.loadt.get()) / ((float) GlobalDatas.cptMaip.get()));
	}
	
	@Override
	public void run() {
		MongoDbAccess dbvitam = null;
		try {
			dbvitam = new MongoDbAccess(mongoClient, "VitamLinks", esbase, unicast, false);
			// now ingest metaaip/metafield/data
			long date11 = System.currentTimeMillis();
			parserIngest = new ParserIngest(simulate);
			parserIngest.parse(ingest);
			parserIngest.executeToFile(dbvitam, start, stop);
			long date12 = System.currentTimeMillis();
			CopyOfMainIngestFile.loadt.addAndGet(date12-date11);
			GlobalDatas.cptMaip.addAndGet(parserIngest.getTotalCount());
			return;
		} catch (InvalidExecOperationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidParseOperationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidUuidOperationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			// release resources
			if (dbvitam != null) {
				dbvitam.close();
			}
		}
	}

}