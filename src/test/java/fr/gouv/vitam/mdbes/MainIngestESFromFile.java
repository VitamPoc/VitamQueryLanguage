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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
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

import fr.gouv.vitam.mdbes.ElasticSearchAccess;
import fr.gouv.vitam.mdbes.MongoDbAccess;

/**
 * @author "Frederic Bregier"
 * 
 */
public class MainIngestESFromFile {
	private static AtomicLong loadt = new AtomicLong(0);
	
	private static MongoClient mongoClient = null;
	
	public static String ingest;
	public static String model;
	public static String host = "localhost";
	public static String esbase = "vitam";
	public static String unicast = "mdb002, mdb003, mdb004";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 6) {
			System.err.println("need: logfile host escluster unicast indextype files");
			return;
		}
		String log4j = args[0];
		PropertyConfigurator.configure(log4j);
		// connect to the local database server
		if (args.length > 1) {
			host = args[1];
		}
		if (args.length > 2) {
			esbase = args[2];
		}
		if (args.length > 3) {
			unicast = args[3];
		}
		if (args.length > 4) {
			model = args[4];
		}
		if (args.length > 5) {
			ingest = args[5];
		}
		MongoDbAccess dbvitam = null;
		try {
			MongoClientOptions options = new MongoClientOptions.Builder().connectionsPerHost(4).build();
			mongoClient = new MongoClient(host, options);
			mongoClient.setReadPreference(ReadPreference.primaryPreferred());
			dbvitam = new MongoDbAccess(mongoClient, "VitamLinks", esbase, unicast, false);
			dbvitam.updateEsIndex(model);
			MainIngestESFromFile.loadt = new AtomicLong(0);
			MainIngestFile.cptMaip.set(0);
			
			runOnce(dbvitam);
			
		} catch (Exception e) {
			System.err.println("ERROR: "+e.getMessage());
			e.printStackTrace();
			
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
		public ToClean(MongoDbAccess dbvitam) {
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
	
	private static final void runOnce(MongoDbAccess dbvitam) throws InterruptedException, InstantiationException, IllegalAccessException, IOException {
		System.out.println("Load starting... ");
		
		long date11 = System.currentTimeMillis();
		
		FileInputStream fstream = new FileInputStream(ingest);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		HashMap<String, String> esIndex = new HashMap<>();
		//Read File Line By Line
		while ((strLine = br.readLine()) != null)   {
			BSONObject bson = (BSONObject) JSON.parse(strLine);
			int nbEs = ElasticSearchAccess.addEsIndex(dbvitam, model, esIndex, bson);
			MainIngestFile.cptMaip.addAndGet(nbEs);
		}
		//Close the input stream
		in.close();
		if (! esIndex.isEmpty()) {
			MainIngestFile.cptMaip.addAndGet(esIndex.size());
			System.out.println("Last bulk ES");
			dbvitam.addEsEntryIndex(true, esIndex, model);
			esIndex.clear();
		}
		
		long date12 = System.currentTimeMillis();
		MainIngestESFromFile.loadt.addAndGet(date12-date11);

		System.out.println("Load ended");
		/*System.out.println("All elements\n================================================================");
		DbVitam.printStructure(dbvitam);*/
		long nbBigM = dbvitam.getDaipSize();
		long nbBigD = dbvitam.getPaipSize();
		System.out.println("\n Big Test ("+MainIngestFile.nbThread + " Threads chacune " + MainIngestFile.nb+" itérations de load, nb MAIP: "+MainIngestFile.cptMaip.get()+") with MAIP: " + nbBigM + " DATA: " + nbBigD
				+ " => Load:" + (MainIngestESFromFile.loadt.get()) / ((float) MainIngestFile.cptMaip.get()));
		
		System.out.println("\nThread;nbLoad;nbTotal;Load");
		System.out.println(MainIngestFile.nbThread+";"+MainIngestFile.cptMaip.get()+";"+nbBigM+";"+(MainIngestESFromFile.loadt.get()) / ((float) MainIngestFile.cptMaip.get()));
	}

}
