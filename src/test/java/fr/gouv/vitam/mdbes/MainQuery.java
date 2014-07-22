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

import org.apache.log4j.PropertyConfigurator;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;

import fr.gouv.vitam.query.exception.InvalidExecOperationException;
import fr.gouv.vitam.query.exception.InvalidParseOperationException;
import fr.gouv.vitam.query.parser.MdEsQueryParser;
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
public class MainQuery {
    private static VitamLogger LOGGER = null;
	private static MongoClient mongoClient = null;
	private static final int MAXTHREAD = 1;
    private static String request;
    private static String host = "localhost";
    private static String database = "VitamLinks";
    private static String esbase = "vitam";
    private static String unicast = "mdb002, mdb003, mdb004";
    private static String typeBase = "Courriel";
    
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 6) {
			System.err.println("need: logfile host database esbase unicast type filequery");
			return;
		}
        final String log4j = args[0];
        PropertyConfigurator.configure(log4j);
        VitamLoggerFactory.setDefaultFactory(new LogbackLoggerFactory(VitamLogLevel.DEBUG));
        LOGGER = VitamLoggerFactory.getInstance(MainQuery.class);
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
        if (args.length > 5) {
            typeBase = args[5];
        }
        if (args.length > 6) {
            request = FileUtil.readFile(args[6]);
        }
		// connect to the local database server
        MongoDbAccess dbvitam = null;
        try {
            final MongoClientOptions options = new MongoClientOptions.Builder().connectionsPerHost(MAXTHREAD).build();
            mongoClient = new MongoClient(host, options);
            mongoClient.setReadPreference(ReadPreference.primaryPreferred());
            dbvitam = new MongoDbAccess(mongoClient, database, esbase, unicast, false);
			runOnce(dbvitam);
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
		LOGGER.warn("Unitary test\n================================================================================================================================");
		try {
			oneShot(dbvitam);
		} catch (InvalidParseOperationException | InvalidExecOperationException e) {
            LOGGER.error(e);
			return;
		}
	}

	protected static void oneShot(MongoDbAccess dbvitam) throws InvalidParseOperationException, InvalidExecOperationException, InstantiationException, IllegalAccessException {
        // Requesting
        long date13 = System.currentTimeMillis();
		String comdtree = request.toString();
		MdEsQueryParser parser = new MdEsQueryParser(false);
		parser.parseQueryOnly(comdtree);
		DbRequest reqdb = new DbRequest(dbvitam, GlobalDatas.INDEXNAME, typeBase);
        reqdb.setUseCache(GlobalDatas.SAVERESULT);
		ResultInterface startSet = MongoDbAccess.createOneResult(GlobalDatas.ROOTS);
        startSet.setMaxLevel(0);
        startSet.setMinLevel(0);
        startSet.putBeforeSave();
		List<ResultInterface> results = reqdb.execQuery(parser, startSet);
		long date14 = System.currentTimeMillis();
		ResultInterface result = reqdb.finalizeResults(parser.hintCache(), results);
		long date15 = System.currentTimeMillis();
		long nbUnitaryM = dbvitam.getDaipSize();
		LOGGER.warn("Unitary Test with DAIP: " + nbUnitaryM 
				+ " => Exec:" + (date14 - date13) +
				" Path:" + (date15 - date14));
		if (result != null && ! result.getCurrentDaip().isEmpty()) {
            LOGGER.warn("Tree= "+result.getCurrentDaip().size());
		    /*for (String id : result.currentDaip) {
	            DAip daip = DAip.findOne(dbvitam, UUID.getLastAsString(id));
                LOGGER.warn("ResTree: "+daip);
            }*/
		} else {
		    LOGGER.error("Tree : no result");
		}
	}
}
