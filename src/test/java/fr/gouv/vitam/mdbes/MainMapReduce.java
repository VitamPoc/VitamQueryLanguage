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
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;

import com.mongodb.BasicDBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceCommand.OutputType;
import com.mongodb.DBObject;
import com.mongodb.MapReduceOutput;
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
public class MainMapReduce {
    private static VitamLogger LOGGER = null;
	private static MongoClient mongoClient = null;
	private static final int MAXTHREAD = 1;
    private static String host = "localhost";
    private static String database = "VitamLinks";
    private static String esbase = "vitam";
    private static String unicast = "mdb002, mdb003, mdb004";
    private static String map = null;
    private static String reduce = null;
    private static String output = null;
    private static String options = null;
    
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("need: logfile host database esbase unicast [map] [reduce] [output] [options-commaEqualities separated list]");
			return;
		}
        final String log4j = args[0];
        PropertyConfigurator.configure(log4j);
        VitamLoggerFactory.setDefaultFactory(new LogbackLoggerFactory(VitamLogLevel.DEBUG));
        LOGGER = VitamLoggerFactory.getInstance(MainMapReduce.class);
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
            map = args[5];
        }
        if (args.length > 6) {
            reduce = args[6];
        }
        if (args.length > 7) {
            output = args[7];
        }
        if (args.length > 8) {
            options = args[8];
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
		try {
			oneShot(dbvitam);
		} catch (InvalidParseOperationException | InvalidExecOperationException e) {
            LOGGER.error(e);
			return;
		}
	}

	protected static final Object getParsedString(String value) {
	    try {
	        return Long.parseLong(value);
	    } catch (Exception e) {
	        try {
	            return Double.parseDouble(value);
	        } catch (Exception e2) {
	            try {
	                return DateTime.parse(value);
	            } catch (Exception e3) {
	                try {
	                    return Boolean.parseBoolean(value);
	                } catch (Exception e4) {
	                    return value;
	                }
	            }
	        }
	    }
	}
	protected static void oneShot(MongoDbAccess dbvitam) throws InvalidParseOperationException, InvalidExecOperationException, InstantiationException, IllegalAccessException {
        // Requesting
	    if (map == null) {
	        map = "function() {"
                + "flattened = serializeDocFiltered(this, MAXDEPTH, FILTER);"
                + "for (var key in flattened) {"
                + "var value = flattened[key];" 
                + "var valueType = varietyTypeOf(value);"
                + "var finalvalue = { types : [ valueType ], occurences : 1};"
//                + "var finalvalue = { occurences : 1};"
                + "emit(key, finalvalue); } }";
	    }
	    if (reduce == null) {
	        reduce = "function(key,values) {"
                + "var typeset = new Set();"
                + "var occur = 0;"
                + "for (var idx = 0; idx < values.length; idx++) {"
                + "occur += values[idx].occurences;"
                + "typeset.add(values[idx].types);" 
                + "} return { types : typeset.asArray(), occurences : occur }; }";
//                + "} return { occurences : occur }; }";

	    }
	    if (output == null) {
	        output = "AttributeUsage";
	    }
        MapReduceCommand mapReduceCommand = new MapReduceCommand(dbvitam.daips.collection, 
                map, reduce, 
                output, OutputType.REDUCE, 
                new BasicDBObject());
        if (options != null) {
            String []optionsArray = options.split(",");
		    Map<String, Object> mapScope = new HashMap<String, Object>();
		    for (String string : optionsArray) {
		        String []kv = string.split("=");
                mapScope.put(kv[0], getParsedString(kv[1]));
            }
		    mapReduceCommand.setScope(mapScope);
        } else {
            Map<String, Object> mapScope = new HashMap<String, Object>();
            mapScope.put("MAXDEPTH", 5);
            mapScope.put("FILTER", "_dds");
            mapReduceCommand.setScope(mapScope);
        }
        mapReduceCommand.addExtraOption("nonAtomic", true);
        mapReduceCommand.addExtraOption("jsMode", true);
        MapReduceOutput output = dbvitam.daips.collection.mapReduce(mapReduceCommand);
        System.out.println("Duration: "+output.getDuration()+
                " Saved into: "+output.getCollectionName()+
                " Input: "+output.getInputCount()+
                " Emit: "+output.getEmitCount()+
                " Output: "+output.getOutputCount()+
                "\nCmd: "+output.getCommand());
        Iterable<DBObject> iterable = output.results();
        for (DBObject dbObject : iterable) {
            System.out.println(dbObject.toString());
        }
	}
}
