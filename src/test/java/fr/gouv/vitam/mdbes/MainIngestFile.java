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
import java.util.List;
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
public class MainIngestFile implements Runnable {
	private static AtomicLong loadt = new AtomicLong(0);
	private static AtomicLong mongoLoad = new AtomicLong(0);
	
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
	private static List<File> files = new ArrayList<File>();
	
	private ParserIngest parserIngest;
	private int start, stop;


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
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 8) {
			System.err.println("need: logfile eraze/noeraze host escluster unicast nbload files start fileout limitdepth mongoimport 0/1 nbThread");
			System.err.println("need: logfile nbload files eraze/noeraze start host escluster unicast fileout limitdepth mongoimport 0/1 (1=stop index)");
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
		}
		if (args.length > 9) {
			int stoplevel = Integer.parseInt(args[9]);
			MainIngestFile.minleveltofile = stoplevel;
		}
		if (args.length > 10) {
			commandMongo = args[10];
		}
		boolean stopindex = false;
		if (args.length > 11) {
			stopindex = args[11].equals("1");
		}
		if (args.length > 12) {
			nbThread = Integer.parseInt(args[12]);
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
			if (MainIngestFile.eraze) {
				reinit(dbvitam);
				return;
			}
			dbvitam.updateEsIndex(model);
			// drop all the data in it
			ParserIngest parser = new ParserIngest(true);
			parser.parse(ingest);
			model = parser.getModel();
			
			int stepnb = realnb;
			MainIngestFile.nb = stepnb;
			MainIngestFile.loadt = new AtomicLong(0);
			MainIngestFile.cptMaip.set(0);
			if (stopindex) {
				dbvitam.removeIndexBeforeImport();
			}
			runOnce(dbvitam);
			// Continue with MongoDB Loading if setup
			if (! MainIngestFile.eraze) {
				if (commandMongo != null) {
					System.out.println("Launch MongoImport");
					runOnceMongo(dbvitam, stopindex);
				}
			}

		} finally {
			// release resources
			ScheduledExecutorService scheduler =
				     Executors.newScheduledThreadPool(2);
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
		MainIngestFile[] ingests = new MainIngestFile[MainIngestFile.nbThread];
		MainIngestFile.nb = MainIngestFile.nb / MainIngestFile.nbThread;
		ExecutorService executorService = null;
		int step = startFrom;
		executorService = Executors.newFixedThreadPool(MainIngestFile.nbThread);
		int interval = MainIngestFile.nb;
		System.out.print("Load starting... "+MainIngestFile.nbThread+":"+MainIngestFile.nb+":"+interval);
		ingests[0] = new MainIngestFile();
		ingests[0].start = step;
		ingests[0].stop = step + interval - 1;
		step += interval;
		executorService.execute(ingests[0]);
		Thread.sleep(100);
		for (int i = 1; i < MainIngestFile.nbThread; i++) {
			ingests[i] = new MainIngestFile();
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
		System.out.println("\n Big Test ("+MainIngestFile.nbThread + " Threads chacune " + MainIngestFile.nb+" itérations de load, nb MAIP: "+MainIngestFile.cptMaip.get()+") with MAIP: " + nbBigM + " DATA: " + nbBigD
				+ " => Load:" + (MainIngestFile.loadt.get()) / ((float) MainIngestFile.cptMaip.get()));
		
		System.out.println("\nThread;nbLoad;nbTotal;Load");
		System.out.println(MainIngestFile.nbThread+";"+MainIngestFile.cptMaip.get()+";"+nbBigM+";"+(MainIngestFile.loadt.get()) / ((float) MainIngestFile.cptMaip.get()));
	}
	
	@Override
	public void run() {
		MongoDbAccess dbvitam = null;
		try {
			dbvitam = new MongoDbAccess(mongoClient, "VitamLinks", esbase, unicast, false);
			// now ingest metaaip/metafield/data
			long date11 = System.currentTimeMillis();
			parserIngest = new ParserIngest(simulate);

			File file = new File(fileout+"-"+this.start+"-"+this.stop+".json");
			FileOutputStream outputStream = new FileOutputStream(file);
			parserIngest.bufferedOutputStream = new BufferedOutputStream(outputStream);

			parserIngest.parse(ingest);
			parserIngest.executeToFile(dbvitam, start, stop);
			long date12 = System.currentTimeMillis();
			MainIngestFile.loadt.addAndGet(date12-date11);
			MainIngestFile.cptMaip.addAndGet(parserIngest.getTotalCount());
			files.add(file);
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
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			// release resources
			if (parserIngest.bufferedOutputStream != null) {
				System.out.println("Flushing file");
				try {
					parserIngest.bufferedOutputStream.flush();
				} catch (IOException e) {
				}
				System.out.println("Closing file");
				try {
					parserIngest.bufferedOutputStream.close();
				} catch (IOException e) {
				}
				System.out.println("File closed");
			}
			if (dbvitam != null) {
				dbvitam.close();
			}
		}
	}

	private static final void runOnceMongo(MongoDbAccess dbvitam, boolean stopindex) throws InterruptedException, InstantiationException, IllegalAccessException, IOException {
		MongoRunImport[] ingests = new MongoRunImport[MainIngestFile.nbThread];
		ExecutorService executorService = null;
		executorService = Executors.newFixedThreadPool(MainIngestFile.nbThread);
		System.out.print("Load starting... "+MainIngestFile.nbThread+":"+MainIngestFile.nb);
		for (int i = 1; i < MainIngestFile.nbThread; i++) {
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
		long start = System.currentTimeMillis();
		if (stopindex) {
			dbvitam.resetIndexAfterImport();
		}
		long stop = System.currentTimeMillis();
		System.out.println("End of MongoImport MaipES:MaipMD:MaipReindex:MaipTotal:MAIP/s");
		System.out.println("End of MongoImport "+
				(MainIngestFile.loadt.get()) / ((float) MainIngestFile.cptMaip.get())+":"+
				(MainIngestFile.mongoLoad.get()) / ((float) MainIngestFile.cptMaip.get())+":"+
				(stop-start) / ((float) MainIngestFile.cptMaip.get())+":"+
				(MainIngestFile.loadt.get()+MainIngestFile.mongoLoad.get()+stop-start) / ((float) MainIngestFile.cptMaip.get())+
				":"+ (MainIngestFile.cptMaip.get() / ((double)(MainIngestFile.loadt.get()+MainIngestFile.mongoLoad.get()+stop-start))));
	}

	private static class MongoRunImport implements Runnable {
		private File file;
		
		public void run() {
			long date11 = System.currentTimeMillis();
			System.out.println("Launch MongoImport");
			Runtime runtime = Runtime.getRuntime();
			String cmd = commandMongo+" -h "+host+" -d VitamLinks -c DAip --upsert --file "+file.getAbsolutePath();
			System.out.println("after removing index, start cmd: "+cmd);
			final Process process;
			try {
				process = runtime.exec(cmd);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				return;
			}
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
			try {
				process.waitFor();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long date12 = System.currentTimeMillis();
			MainIngestFile.mongoLoad.addAndGet(date12-date11);
		}
	}
}
