/**
   This file is part of Waarp Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All Waarp Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   Waarp is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Waarp .  If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.mdbes;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;

import fr.gouv.vitam.query.exception.InvalidExecOperationException;
import fr.gouv.vitam.query.exception.InvalidParseOperationException;
import fr.gouv.vitam.query.exception.InvalidUuidOperationException;
import fr.gouv.vitam.utils.FileUtil;
import fr.gouv.vitam.utils.GlobalDatas;

/**
 * @author "Frederic Bregier"
 *
 */
public class QueryBenchJavaSampler extends AbstractJavaSamplerClient {
	public static enum ARG {
		mongohost, mongobase, maxconn, esclustername, unicast, modelfile, serie, start, stop, maxes;
	}
	
	static final ReentrantLock lock = new ReentrantLock();
	static MongoClient mongoClient = null;
	static MongoClientOptions options = null;
	static final ThreadLocal<BenchContext> threadBenchContext = new ThreadLocal<>();
	
	int error = 0;
	boolean success = false;
	long count = 0;
	
	MongoDbAccess dbvitam = null;
	ParserBench parserBench = null;
	String mongohost = "localhost";
	String mongobase = "VitamLinks";
	int maxconn = 20;
	String esclustername = "vitam";
	String unicast = "mdb002, mdb003, mdb004";
	String modelfile = "D:/opt/PocMongoDBElasticSearch/src/main/Modèles/Télégramme/Telegramme-request-2.json";
	String modelQuery;
	String model = "unknown";
	String serie = "0";
	int start = 0;
	int stop = 9;
	long current = 0;
	long maxES = 10001;
	boolean initialized = false;
	
	@Override
	public Arguments getDefaultParameters() {
		Arguments args = new Arguments();
		args.addArgument("mongohost", "localhost");
		args.addArgument("mongobase", "VitamLinks");
		args.addArgument("maxconn", "200");
		args.addArgument("esclustername", "vitam");
		args.addArgument("unicast", "mdb002, mdb003, mdb004");
		args.addArgument("modelfile", "D:/opt/PocMongoDBElasticSearch/src/main/Modèles/Télégramme/Telegramme-request-2.json");
		args.addArgument("serie", "tree");
		args.addArgument("eraze", "1");
		args.addArgument("start", "0");
		args.addArgument("stop", "9");
		args.addArgument("maxes", "10001");

		return args;
	}

	@Override
	public SampleResult runTest(JavaSamplerContext arg0) {
		// called when the test is startup but only for global argument, not specific ones
		if (arg0.containsParameter(ARG.maxes.name())) {
			maxES = arg0.getIntParameter(ARG.maxes.name());
			GlobalDatas.limitES = maxES;
		}
		error = 0;
		count = 0;
		success = false;
		String msg = "";
		if (! initialized) {
			error ++;
			long stamp = System.currentTimeMillis();
			long second = stamp+1;
			SampleResult sr = new SampleResult();
			sr.setSampleLabel(model+" "+serie);
			sr.setStampAndTime(stamp, second-stamp);
			//sr.setTimeStamp(stamp);
			//sr.setEndTime(second);
			sr.setErrorCount(error);
			msg = "Not Initialized";
			sr.setResponseMessage(msg);
			sr.setResponseCode("501");
			success = false;
			count = 0;
			sr.setSampleCount((int) count);
			sr.setSuccessful(success);
			return sr;
		}
		if (threadBenchContext.get() == null) {
			threadBenchContext.set(parserBench.getNewContext(GlobalDatas.INDEXNAME, model));
		}
		BenchContext context = threadBenchContext.get();
		if (context.cpts.isEmpty()) {
			// wrong initialization
			threadBenchContext.set(parserBench.getNewContext(GlobalDatas.INDEXNAME, model));
			context = threadBenchContext.get();
		}
		if (current < start || current > stop) {
			current = start;
		}
		long stamp = System.currentTimeMillis();
		// run test
		try {
		    List<ResultCached> results = parserBench.executeBenchmark(dbvitam, current, context);
            ResultCached result = parserBench.finalizeResults(dbvitam, context, results);
			count++;
		} catch (InvalidExecOperationException e) {
			msg = e.getMessage();
			error++;
		} catch (InstantiationException e) {
            msg = e.getMessage();
            error++;
        } catch (IllegalAccessException e) {
            msg = e.getMessage();
            error++;
        } catch (InvalidParseOperationException e) {
            msg = e.getMessage();
            error++;
        }
		long second = System.currentTimeMillis();
		SampleResult sr = new SampleResult();
		sr.setSampleLabel(model+" "+serie);
		sr.setStampAndTime(stamp, second-stamp);
		//sr.setEndTime(second);
		sr.setErrorCount(error);
		if (error > 0) {
			sr.setResponseMessage(msg);
			sr.setResponseCode("502");
			success = false;
		} else {
			sr.setResponseOK();
			success = true;
		}
		if (count > Integer.MAX_VALUE) {
			sr.setSampleCount(Integer.MAX_VALUE);
		} else {
			sr.setSampleCount((int) count);
		}
		sr.setSamplerData(""+current);
		sr.setSuccessful(success);
		current++;
		if (current > stop) {
			current = start;
		}
		return sr;
	}

	@Override
	public void setupTest(JavaSamplerContext arg0) {
		// called when the test is startup but only for global argument, not specific ones
		for (ARG arg : ARG.values()) {
			if (arg0.containsParameter(arg.name())) {
				switch (arg) {
					case maxes:
						maxES = arg0.getIntParameter(arg.name());
						GlobalDatas.limitES = maxES;
						break;
					case modelfile:
						modelfile = arg0.getParameter(arg.name()).trim();
						try {
							modelQuery = FileUtil.readFile(modelfile);
						} catch (IOException e) {
							e.printStackTrace();
							modelQuery = null;
						}
						break;
					case start:
						start = arg0.getIntParameter(arg.name());
						current = start;
						break;
					case stop:
						stop = arg0.getIntParameter(arg.name());
						break;
					case serie:
						serie = arg0.getParameter(arg.name()).trim();
					default:
						break;
				}
			}
		}
		if (initialized) {
			parserBench = new ParserBench(false);
			try {
				parserBench.prepareParse(modelQuery);
			} catch (InvalidParseOperationException e) {
				e.printStackTrace();
			}
			model = parserBench.getModel();
			return;
		}
		// called when the test is startup but only for global argument, not specific ones
		for (ARG arg : ARG.values()) {
			if (arg0.containsParameter(arg.name())) {
				switch (arg) {
					case esclustername:
						esclustername = arg0.getParameter(arg.name()).trim();
						break;
					case unicast:
						unicast = arg0.getParameter(arg.name()).trim();;
						break;
					case maxconn:
						maxconn = arg0.getIntParameter(arg.name());
						break;
					case mongobase:
						mongobase = arg0.getParameter(arg.name()).trim();
						break;
					case mongohost:
						mongohost = arg0.getParameter(arg.name()).trim();
						break;
					default:
						break;
				}
			}
		}
		lock.lock();
		try {
			if (modelQuery != null) {
				try {
					if (mongoClient == null) {
						options = new MongoClientOptions.Builder().connectionsPerHost(maxconn).build();
						mongoClient = new MongoClient(mongohost, options);
						mongoClient.setReadPreference(ReadPreference.primaryPreferred());
					}
					dbvitam = new MongoDbAccess(mongoClient, mongobase, esclustername, unicast, false);
					parserBench = new ParserBench(false);
					parserBench.prepareParse(modelQuery);
					model = parserBench.getModel();
					initialized = true;
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (InvalidParseOperationException e) {
					e.printStackTrace();
				} catch (InvalidUuidOperationException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } finally {
					if (! initialized) {
						if (dbvitam != null) {
							dbvitam.close();
							dbvitam = null;
						}
						if (mongoClient != null) {
							mongoClient.close();
							mongoClient = null;
						}
					}
				}
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void teardownTest(JavaSamplerContext arg0) {
		if (!initialized) {
			return;
		}
		// called at end of runTest
		if (dbvitam != null) {
			dbvitam.close();
			dbvitam = null;
		}
		lock.lock();
		try {
			if (mongoClient != null) {
				mongoClient.close();
				mongoClient = null;
			}
		} finally {
			lock.unlock();
		}
		initialized = false;
	}

}
