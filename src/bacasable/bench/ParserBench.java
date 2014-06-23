/**
 * This file is part of POC MongoDB ElasticSearch Project.
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 * 
 * POC MongoDB ElasticSearch is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with POC MongoDB
 * ElasticSearch . If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.query.old.exec.bench;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.BSONObject;
import org.bson.types.BasicBSONList;

import test.mongodb.parserCommand.ParserCommand;
import test.mongodb.parserCommand.TypeField;
import test.mongodb.utils.DbVitam;
import test.mongodb.utils.FileUtil;

import com.mongodb.util.JSON;

import fr.gouv.vitam.mdbtypes.DAip;
import fr.gouv.vitam.mdbtypes.MongoDbAccess;
import fr.gouv.vitam.mdbtypes.MongoDbAccess.VitamCollections;
import fr.gouv.vitam.query.exception.InvalidExecOperationException;
import fr.gouv.vitam.query.exception.InvalidParseOperationException;
import fr.gouv.vitam.query.parser.MdEsQueryParser;
import fr.gouv.vitam.query.parser.TypeRequest;

/**
 * @author "Frederic Bregier"
 * 
 */
public class ParserBench extends MdEsQueryParser {
    public static final String CPTLEVEL = "__cptlevel__";

    public static final String VARY = "vary";
    public static enum FIELD {
        save, liste, listeorder, serie, interval, setdistrib
    }

    public static enum FIELD_ARGS { 
        __name, __type,
        __liste, __listeorder, __serie, __prefix, __idcpt, __modulo, __low, __high, __subprefix, __save
    }

    BenchContext context = new BenchContext();
    AtomicLong distribCpt = null;
    
    public ParserBench(boolean simul) {
        super(simul);
    }
    
    @Override
    public void parse(String request) throws InvalidParseOperationException {
        this.request = request;
        BasicBSONList requests = (BasicBSONList) JSON.parse(request);
        int nbreq = requests.size();
        if (nbreq == 0) {
            return;
        }
        int level = 0;
        for (Object reqb : requests) {
            BSONObject reqbson = (BSONObject) reqb;
            if (this.model == null) {
                this.model = (String) reqbson.get("$"+START.model.name());
            }
            if (reqbson.containsField("$"+START.domain.name())) {
                // domain
                TypeRequest tr = analyzeDomain((BSONObject) reqbson.get("$"+START.domain.name()));
                context.cpts.put(CPTLEVEL+level, new AtomicLong(0));
                // now check variabilisation
                analyszeVariabilisation(reqbson, tr, level);
            } else if (reqbson.containsField("$"+START.maip.name())) {
                // maip
                TypeRequest tr = analyzeMaip((BSONObject) reqbson.get("$"+START.maip.name()));
                context.cpts.put(CPTLEVEL+level, new AtomicLong(0));
                // now check variabilisation
                analyszeVariabilisation(reqbson, tr, level);
            } else {
                // error
                throw new InvalidParseOperationException("Unknown type of request: "+reqbson.keySet());
            }
            level++;
        }
    }
        
    private void analyszeVariabilisation(BSONObject req, TypeRequest tr, int level) throws InvalidParseOperationException {
        tr.fields = new ArrayList<>();
        if (req.containsField(VARY)) {
            BasicBSONList variab = (BasicBSONList) req.get(VARY);
            for (int i = 0; i < variab.size(); i++) {
                BSONObject bson = (BSONObject) variab.get(i);
                TypeField tf = getField(bson, level);
                if (tf != null) {
                    tr.fields.add(tf);
                }
            }
        }
    }
    
    private TypeField getField(BSONObject bfield, int level) throws InvalidParseOperationException {
        String name = (String) bfield.get(FIELD_ARGS.__name.name());
        String type = (String) bfield.get(FIELD_ARGS.__type.name());
        if (type == null || type.isEmpty()) {
            if (debug) System.err.println("Unknown empty type: "+type);
            throw new InvalidParseOperationException("Unknown empty type: "+type);
        }
        TypeField field = new TypeField();
        FIELD fieldType = null;
        try {
            fieldType = FIELD.valueOf(type);
        } catch (IllegalArgumentException e) {
            if (debug) System.err.println("Unknown type: "+bfield);
            throw new InvalidParseOperationException("Unknown type: "+bfield);
        }
        field.name = name;
        field.type = fieldType;
        switch (fieldType) {
            case setdistrib: {
                // no field but CPT level
                distribCpt = context.cpts.get(CPTLEVEL+level);
                return null;
            }
            case save: {
                break;
            }
            case liste:
            case listeorder: {
                BasicBSONList liste = (BasicBSONList) bfield.get("__"+fieldType.name());
                if (liste == null || liste.isEmpty()) {
                    if (debug) System.err.println("Empty List: "+liste);
                    throw new InvalidParseOperationException("Empty List: "+bfield);
                }
                field.listeValeurs = new String[liste.size()];
                for (int i = 0; i < liste.size(); i++) {
                    field.listeValeurs[i] = (String) liste.get(i);
                }
                break;
            }
            case serie: {
                BSONObject bson = (BSONObject) bfield.get(FIELD_ARGS.__serie.name());
                if (bson == null) {
                    if (debug) System.err.println("Empty serie: "+bfield);
                    throw new InvalidParseOperationException("Empty serie: "+bfield);
                }
                if (bson.containsField(FIELD_ARGS.__prefix.name())) {
                    String prefix = (String) bson.get(FIELD_ARGS.__prefix.name());
                    if (prefix == null) {
                        prefix = "";
                    }
                    field.prefix = prefix;
                }
                if (bson.containsField(FIELD_ARGS.__idcpt.name())) {
                    String idcpt = (String) bson.get(FIELD_ARGS.__idcpt.name());
                    if (idcpt != null && ! idcpt.isEmpty()) {
                        field.idcpt = idcpt;
                        context.cpts.put(idcpt, new AtomicLong(0));
                    }
                }
                field.modulo = -1;
                if (bson.containsField(FIELD_ARGS.__modulo.name())) {
                    int modulo = (int) bson.get(FIELD_ARGS.__modulo.name());
                    if (modulo > 0) {
                        field.modulo = modulo;
                    }
                }
                break;
            }
            case interval: {
                Integer low = (Integer) bfield.get(FIELD_ARGS.__low.name());
                if (low == null) {
                    if (debug) System.err.println("Empty interval: "+bfield);
                    throw new InvalidParseOperationException("Empty interval: "+bfield);
                }
                Integer high = (Integer) bfield.get(FIELD_ARGS.__high.name());
                if (high == null) {
                    if (debug) System.err.println("Empty interval: "+bfield);
                    throw new InvalidParseOperationException("Empty interval: "+bfield);
                }
                field.low = low;
                field.high = high;
                break;
            }
            default:
                if (debug) System.err.println("Incorrect type: "+bfield);
                throw new InvalidParseOperationException("Incorrect type: "+bfield);
        }
        if (bfield.containsField(FIELD_ARGS.__save.name())) {
            String savename = (String) bfield.get(FIELD_ARGS.__save.name());
            if (savename != null && ! savename.isEmpty()) {
                field.saveName = savename;
            }
        }
        if (bfield.containsField(FIELD_ARGS.__subprefix.name())) {
            BasicBSONList liste = (BasicBSONList) bfield.get(FIELD_ARGS.__subprefix.name());
            if (liste == null || liste.isEmpty()) {
                System.err.println("Empty SubPrefix List: "+liste);
                throw new InvalidParseOperationException("Empty SubPrefix List: "+bfield);
            }
            field.subprefixes = new String[liste.size()];
            for (int i = 0; i < liste.size(); i++) {
                field.subprefixes[i] = (String) liste.get(i);
            }
        }
        return field;
    }

    public static String getFinalRequestMD(TypeRequest request, AtomicLong rank, BenchContext bench) {
        String finalRequest = request.requestModel[MongoDB].toString();
        if (finalRequest == null) {
            return null;
        }
        return getRequest(finalRequest, request, rank, bench);
    }
    
    public static String getFinalRequestES(TypeRequest request, AtomicLong rank, BenchContext bench) {
        String finalRequest = request.requestModel[ElasticSearch].toString();
        if (finalRequest == null) {
            return null;
        }
        return getRequest(finalRequest, request, rank, bench);
    }
    
    public static String getFinalFilter(TypeRequest request, AtomicLong rank, BenchContext bench) {
        String finalRequest = request.filterModel[ElasticSearch].toString();
        if (finalRequest == null) {
            return null;
        }
        return getRequest(finalRequest, request, rank, bench);
    }

    private static String getValue(TypeField typeField, String curval, Map<String, String> savedNames) {
        String finalVal = curval;
        if (typeField.subprefixes != null) {
            String prefix = "";
            for (String name : typeField.subprefixes) {
                String tempval = savedNames.get(name);
                if (tempval != null) {
                    prefix += tempval;
                } else {
                    prefix += name;
                }
            }
            finalVal = prefix + finalVal;
        }
        if (typeField.saveName != null) {
            savedNames.put(typeField.saveName, finalVal);
        }
        return finalVal;
    }
    
    private static String getRequest(String finalRequest, TypeRequest request, AtomicLong rank, BenchContext bench) {
        if (request.fields != null && ! request.fields.isEmpty()) {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            for (TypeField field : request.fields) {
                String val = null;
                switch (field.type) {
                    case save:
                        val = getValue(field, "", bench.savedNames);
                        finalRequest = finalRequest.replace(field.name, val);
                        break;
                    case liste:
                        int rlist = rnd.nextInt(field.listeValeurs.length);
                        val = field.listeValeurs[rlist];
                        val = getValue(field, val, bench.savedNames);
                        finalRequest = finalRequest.replace(field.name, val);
                        break;
                    case listeorder:
                        long i = rank.getAndIncrement();
                        if (i >= field.listeValeurs.length) {
                            i = field.listeValeurs.length-1;
                        }
                        val = field.listeValeurs[(int) i];
                        val = getValue(field, val, bench.savedNames);
                        finalRequest = finalRequest.replace(field.name, val);
                        break;
                    case serie:
                        AtomicLong newcpt = rank;
                        if (field.idcpt != null) {
                            newcpt = bench.cpts.get(field.idcpt);
                            if (newcpt == null) {
                                newcpt = rank;
                                System.err.println("wrong cpt name: "+field.idcpt);
                            }
                        }
                        long j = newcpt.getAndIncrement();
                        if (field.modulo > 0) {
                            j = j % field.modulo;
                        }
                        val = (field.prefix != null ? field.prefix : "")+j;
                        val = getValue(field, val, bench.savedNames);
                        finalRequest = finalRequest.replace(field.name, val);
                        break;
                    case interval:
                        int newval = rnd.nextInt(field.low, field.high+1);
                        val = getValue(field, ""+newval, bench.savedNames);
                        finalRequest = finalRequest.replace(field.name, val);
                        break;
                    default:
                        break;
                }
            }
        }
        return finalRequest;
    }
    
    public BenchContext getNewContext() {
        BenchContext newBenchContext = new BenchContext();
        newBenchContext.savedNames.putAll(context.savedNames);
        for (String key : context.cpts.keySet()) {
            AtomicLong cpt = context.cpts.get(key);
            if (distribCpt != null && distribCpt == cpt) {
                AtomicLong count = new AtomicLong(0);
                newBenchContext.cpts.put(key, count);
                newBenchContext.distrib = count;
            } else {
                newBenchContext.cpts.put(key, new AtomicLong(0));
            }
        }
        return newBenchContext;
    }
    
    public void execute(MongoDbAccess dbvitam, boolean debug, long start, BenchContext newBenchContext) throws InvalidExecOperationException {
        this.dbvitam = dbvitam;
        RequestBench requestBench = new RequestBench();
        requestBench.simulate = simulate;
        int level = 0;
        if (newBenchContext.distrib != null) {
            newBenchContext.distrib.set(start);
        }
        AtomicLong rank = newBenchContext.cpts.get(CPTLEVEL+level);
        /*if (rank != null && rank != newBenchContext.distrib) {
            rank.set(0);
        }*/
        /*
        AtomicLong rank = context.cpts.get(CPTLEVEL+level);
        
        if (distribCpt != null && distribCpt == rank) {
            if (debug) System.err.println("set start: "+start);
            distribCpt.set(start);
        } else {
            rank.set(0);
        }
        */
        for (TypeRequest request : requests) {
            int nb = requestBench.executeRequest(dbvitam, request, rank, newBenchContext, model, debug);
            if (nb <= 0) {
                System.err.println("Error on execute with 0 out for: "+request.toString()+"["+rank+"]");
                throw new InvalidExecOperationException("No result");
            } else {
                if (debug) System.out.println("Current: "+nb+":"+level);
            }
            level++;
            rank = newBenchContext.cpts.get(CPTLEVEL+level);
            /*if (rank != null && rank != newBenchContext.distrib) {
                rank.set(0);
            }*/
            /*
            rank = context.cpts.get(CPTLEVEL+level);
            if (distribCpt != null && distribCpt == rank) {
                if (debug) System.err.println("set start: "+start);
                distribCpt.set(start);
            } else if (rank != null) {
                rank.set(0);
            }
            */
        }
        results = requestBench.resultRequest;
        int nb = results.size();
        if (debug && nb > 1) {
            System.out.println("MORE THAN ONE RESULT: "+nb);
        }
        if (debug) System.out.println("Result number: "+nb);
        if (! simulate && debug && nb > 0) {
            DAip maip = null;
            for (String id : results.lastIds) {
                maip = (DAip) dbvitam.loadFromObjectId(VitamCollections.Cdaip.vcollection, id);
                if (maip != null) {
                    maip.getAfterLoad();
                    try {
                        DbVitam.printMaip(dbvitam, maip, 1, debug);
                    } catch (InstantiationException | IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
    
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Model: "+model);
        for (TypeRequest request : requests) {
            builder.append("\n"+request.toString());
        }
        return builder.toString();
    }
    
    private static final String exampleBothEsMd = "[ { $model : 'modeltype', $domain: { $term: { field0: 'termval' } } } , " +
            "{ $maip : { $nested : { path : 'field1', $and : [ { $in : { field1.field2 : [ 'val1', 'val2' ] } }," +
            " { $exists : 'field3' }, { $missing : 'field4' }, { $limit : 10 }, { $range : { field5 : { from: 2, to: 5 } } }, " +
            "{ $range : { field6 : { gt: 2, lt: 5 } } } ] } }, " +
            VARY+" : [ { __name : 'val1', __type : 'liste', __liste : ['aaa', 'bbb'] }, { __name : 'val2', __type : 'listeorder', __listeorder : ['ccc', 'ddd'] } ] }" +
            ", { $maip : { $depth : { depth : 5, $match : { field6: 'val6' } } }, " +
            VARY+" : [ { __name : 'val6', __type : 'serie', __serie : { __prefix : 'Pref_', __idcpt : 'moncpt', __modulo: 100 } } ] }" +
            ", { $maip : { $flt: { fields: [ 'field7', 'field8' ], like_text: 'liketext8', stop_words: 'stopwords' } }, " +
            VARY+" : [ { __name : 'liketext8', __type : 'serie', __serie : { __prefix : 'Text_', __idcpt : 'moncpt2' } } ] }" +
            ", { $maip : { $regexp: { field9: { $regex: '[a-z]*', $options : 'i' } } } }" +
            " ]";

    public static void main(String []args) {
        String comd = exampleBothEsMd;
        if (args.length < 1) {
            System.err.println("Need a request as argument => will use default test example");
        } else {
            try {
                comd = FileUtil.readFile(args[0]);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }
        try {
            ParserBench command = new ParserBench(true);
            command.parse(comd);
            BenchContext contextTree = command.getNewContext();
            System.out.println(command.request);
            System.out.println(command.toString());
            System.out.println("Simulate Execution\n=====================");
            for (int i = 0; i < 10; i++) {
                command.execute(null, true, i, contextTree);
            }
        } catch (InvalidParseOperationException e) {
            e.printStackTrace();
        } catch (InvalidExecOperationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
}
