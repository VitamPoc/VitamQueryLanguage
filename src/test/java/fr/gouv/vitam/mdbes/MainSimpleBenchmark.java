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

import org.apache.log4j.PropertyConfigurator;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;

import fr.gouv.vitam.query.exception.InvalidExecOperationException;
import fr.gouv.vitam.query.exception.InvalidParseOperationException;
import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.logging.LogbackLoggerFactory;
import fr.gouv.vitam.utils.logging.VitamLogLevel;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * @author "Frederic Bregier"
 * 
 */
public class MainSimpleBenchmark {
    private static VitamLogger LOGGER = null;
	private static MongoClient mongoClient = null;
	private static final int MAXTHREAD = 1;
    private static String host = "localhost";
    private static String database = "VitamLinks";
    private static String esbase = "vitam";
    private static String unicast = "mdb002, mdb003, mdb004";
    private static int mode = 0;
    private static int [] results = new int[3];
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
			System.err.println("need: logfile host database esbase unicast");
			return;
		}
        final String log4j = args[0];
        PropertyConfigurator.configure(log4j);
        VitamLoggerFactory.setDefaultFactory(new LogbackLoggerFactory(VitamLogLevel.WARN));
        LOGGER = VitamLoggerFactory.getInstance(MainSimpleBenchmark.class);
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
		// connect to the local database server
        MongoDbAccess dbvitam = null;
        try {
            final MongoClientOptions options = new MongoClientOptions.Builder().connectionsPerHost(MAXTHREAD).build();
            mongoClient = new MongoClient(host, options);
            mongoClient.setReadPreference(ReadPreference.primaryPreferred());
            dbvitam = new MongoDbAccess(mongoClient, database, esbase, unicast, false);
            for (mode = 0; mode <= 2; mode++) {
                results[mode] = 0;
            }
            for (int i = 0; i < 10; i++) {
                for (mode = 0; mode <= 2; mode++) {
                    oneShot(dbvitam);
                }
            }
            for (int i = 0; i < 10; i++) {
                for (mode = 0; mode <= 2; mode++) {
                    runOnce(dbvitam);
                }
            }
            for (mode = 0; mode <= 2; mode++) {
                LOGGER.warn("FinalTime: "+mode+" = "+results[mode]);
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
		long start = System.currentTimeMillis();
		for (int i = 0; i < 2000; i++) {
    		try {
    			oneShot(dbvitam);
    		} catch (InvalidParseOperationException | InvalidExecOperationException e) {
                LOGGER.error(e);
    			return;
    		}
		}
        long stop = System.currentTimeMillis();
        results[mode] += (stop-start);
        LOGGER.warn("Time: "+mode+" = "+(stop-start));
	}

	private static final BasicDBObject ID_NBCHILD = new BasicDBObject(VitamType.ID, 1);
    private static final BasicDBObject QUERY = new BasicDBObject(VitamType.ID, "O73_F4jYACd71LMAAUdEN6fR");

	protected static void oneShot(MongoDbAccess dbvitam) throws InvalidParseOperationException, InvalidExecOperationException, InstantiationException, IllegalAccessException {
        // Requesting
	    if (mode == 0) {
    		final DBCursor cursor = dbvitam.daips.collection.find(QUERY, ID_NBCHILD);
            if (cursor.hasNext()) {
                LOGGER.debug("ok");
            }
            cursor.close();
	    } else if (mode == 1) {
	        DBObject obj = dbvitam.daips.collection.findOne(QUERY, ID_NBCHILD);
	        if (obj != null) {
	            LOGGER.debug("ok");
	        }
	    } else if (mode == 2) {
	        long nb = dbvitam.daips.collection.count(QUERY);
	        if (nb > 0) {
                LOGGER.debug("ok");
            }
	    }
	}
}
