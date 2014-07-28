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
package fr.gouv.vitam.mdbes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.exception.InvalidExecOperationException;
import fr.gouv.vitam.query.exception.InvalidParseOperationException;
import fr.gouv.vitam.query.json.JsonHandler;
import fr.gouv.vitam.query.parser.MdEsQueryParser;
import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * 
 * <pre>
 * {@code
===========
| Request |
===========

Valeurs fixes: valeur
=====================
    valeurSimple = "chaine" | nombre
        ou "chaine" = une chaine de caractere fixe
        ou nombre = un nombre fixe
        
    valeur = valeurSimple | listeValeurs
        ou listeValeurs = [ valeurSimple, ... ]

L'expression des champs: champs
===============================
    nomfield = "chaine"
    champ = nomfield : valeur | typeSimple | typeComplexe
        => associe a un nom de champ (chaine) une valeur ou un type simple ou un type complexe
    
    champs = champ | champ, champs
        La notation "champ, champ, ..." signifie serie de champs (exemple : { champ1 : val1, champ2 : val2, champ3 : val3 } )

L'expression des requetes : requests
====================================
    requests = [ subrequests ]
    subrequests = subrequest1 | subrequest1, subrequests2  (mais avec une seule fois $domain autorise)
    subrequests2 = subrequest | subrequest, subrequests2  (mais avec une seule fois $domain autorise)
    subrequest1 = { $model : model, $domain : { request } } | { $model : model, $maip : { request } }
    subrequest = { $maip : { request } }
        les requetes sur $domain ne peuvent concerner que le champ "name", $model ne doit apparaître qu'une seule fois, au plus haut

L'expression d'une requete : request
====================================
    request = reqdepth | reqterm | reqnested | reqin | reqbool | reqexist | reqmissing | reqrange | 
            reqmatch | reqflt | reqmlt | reqfield | reqwildcard | reqlimit
    
    (*)reqdepth = $depth : { depth: nombre, request }
    reqterm = $term : { nomfield : valeurSimple }
    reqnested = $nested : { path : nomfield, request }  ou request exprimera les champs avec nomfield.X
    reqin = $in : { nomfield : valeurSimple }
    reqbool = bool : [ subrequests ]
        bool = $and | $or | $not
    reqexist = $exists : nomfield 
    reqmissing = $missing : nomfield
    reqrange = $range : { nomfield : { rangeconditions } }
        rangeconditions = rangecondition | rangecondition, rangeconditions
        rangecondition = from|to|gt|gte|lt|lte : valeurSimple | include_lower : bool | include_upper : bool
            (defaut : include_lower | include_upper = true)
    (*)reqmatch = ($match | $match_phrase ) : { nomfield : valeurSimple } | 
        $match_phrase_prefix : { nomfield : valeurSimple, max_expansions : nombre } | 
        $multi_match : { query : valeurSimple, fields : [ nomfield, ... ] }
    (*)reqflt = $flt : { fields : [ nomfield, ... ], like_text : valeurSimple } | $flt_field : { nomfield : { like_text : valeurSimple } }
    (*)reqmlt = $mlt : { fields : [ nomfield, ... ], like_text : valeurSimple [, stop_words : listeValeurs] } | 
        $mlt_field : { nomfield : { like_text : valeurSimple [, stop_words : listeValeurs] } }
    (*)reqfield = $field : { nomfield : valeurSimple }
        ou valeurSimple peut contenir : +/- (obligatoire/sauf) asteriks/? (serie de caracteres / 1 caractere) AND/OR (surtout le OR car +=AND)
        NB: si asteriks/? => analyze_wildcard : true
    (*)reqwildcard = $wildcard : { nomfield : valeurSimple }
        ou valeurSimple peut contenir : asteriks/? (serie de caracteres / 1 caractere)
    (+)reqregex = $regexp : { field : { $regex : exprReg, $options : o } }
        NB : garde cette possibilite ?
    reqlimit = $limit : number
    
(*) ES only, (+) MD only




================
| DepthRequest |
================

Valeurs fixes: valeur
=====================
    valeurSimple = "chaine" | nombre
        ou "chaine" = une chaine de caractere fixe
        ou nombre = un nombre fixe
        
    valeur = valeurSimple | listeValeurs
        ou listeValeurs = [ valeurSimple, ... ]

L'expression des champs: champs
===============================
    nomfield = "chaine"
    champ = nomfield : valeur | idref
        => associe a un nom de champ (chaine) une valeur ou un type simple ou un type variable (idref)
    
    champs = champ | champ, champs
        La notation "champ, champ, ..." signifie serie de champs (exemple : { champ1 : val1, champ2 : val2, champ3 : val3 } )
        
Types complexes: variabilisation
================================
    variabilisation = vary : [ types ]
    types = type | type, types
    
    type = { "__name" : "idref", "__type" : "interval", "__low" : valeur, "__high" : valeur }
        => prend une valeur aléatoire entre low et high (inclus)
    
    type = { "__name" : "idref", "__type" : "liste", "__liste" : listeValeurs }
        => de maniere aleatoire, un element de la liste sera attribue au champ

    type = { "__name" : "idref", "__type" : "listeorder", "__listeorder" : listeValeurs }
        => le premier objet du pere commun aura pour valeur la valeur de rang 1, le deuxieme la valeur de rang 2, etc. 
        Si le nombre de fils du pere depasse le nombre de valeurs dans la liste, la derniere valeur est re-utilisee.
        Exemple : { champ : { "__type" : "listeorder", "__listeorder" : [ "A", "B" ] } } avec 5 fils de ce type (occurence._occur = 3)
        => premier fils : { champ : "A" }
        => deuxieme fils : { champ : "B" }
        => troisieme fils : { champ : "B" }

    type = { "__name" : "idref", "__type" : "serie", "__serie" : { "__prefix" : chaine, "__idcpt" : "nomDuCompteur", "__modulo" : nombre } }
        => une chaine aleatoire sera produite du type : "prefixe"+Valeur" ou Valeur sera
        __idcpt est precise : la valeur courante du compteur specifie est utilisee
        __idcpt non precise : la valeur courante du compteur de reference est utilisee
        __modulo est precise : la valeur est calcule par nombre = (nombre % modulo)+1
        __modulo n'est pas precise : la valeur est non modifiee
        Exemple : { champ : { "__type" : "serie", "__serie" : { "__prefix" : "Pref_", "__idcpt" : "moncpt", "__modulo" : 3 } } } avec __occur = 4 et moncpt = 10 au depart
        => premier fils : moncpt = 11 => (11 mod 3) +1 = 3 => { champ : "Pref_3" }
        => deuxieme fils : moncpt = 12 => (12 mod 3) +1 = 1 => { champ : "Pref_1" }
        => troisieme fils : moncpt = 13 => (13 mod 3) +1 = 2 => { champ : "Pref_2" }
        => quatrieme fils : moncpt = 14 => (14 mod 3) +1 = 3 => { champ : "Pref_3" }

L'expression des requetes : requests
====================================
    requests = [ subrequests ]
    subrequests = subrequest | subrequest, subrequests  (mais avec une seule fois $domain autorise)
    subrequest = { $domain : { request }, variabiliation } | { $maip : { request }, variabiliation }
        les requetes sur $domain ne peuvent concerner que le champ "name"

L'expression d'une requete : request
====================================
    request = reqdepth | reqterm | reqnested | reqin | reqbool | reqexist | reqmissing | reqrange | 
            reqmatch | reqflt | reqmlt | reqfield | reqwildcard | reqlimit
    
    (*)reqdepth = $depth : { depth: nombre, request }
    (=)reqterm = $term : { nomfield : typeComplexe }
    reqnested = $nested : { path : nomfield, request }  ou request exprimera les champs avec nomfield.X
    reqin = $in : { nomfield : listeValeurs }
    reqbool = bool : [ subrequests ]
        bool = $and | $or | $not
    reqexist = $exists : nomfield 
    reqmissing = $missing : nomfield
    (=)reqrange = $range : { nomfield : { rangeconditions } }
        rangeconditions = rangecondition | rangecondition, rangeconditions
        rangecondition = from|to|gt|gte|lt|lte : typeComplexe | include_lower : bool | include_upper : bool
            (defaut : include_lower | include_upper = true)
    (=)(*)reqmatch = ($match | $match_phrase ) : { nomfield : typeComplexe } | 
        $match_phrase_prefix : { nomfield : typeComplexe, max_expansions : nombre } | 
        $multi_match : { query : typeComplexe, fields : [ nomfield, ... ] }
    (=)(*)reqflt = $flt : { fields : [ nomfield, ... ], like_text : typeComplexe } | $flt_field : { nomfield : { like_text : typeComplexe } }
    (=)(*)reqmlt = $mlt : { fields : [ nomfield, ... ], like_text : typeComplexe [, stop_words : listeValeurs] } | 
        $mlt_field : { nomfield : { like_text : typeComplexe [, stop_words : listeValeurs] } }
    (=)(*)reqfield = $field : { nomfield : valeurSimple }
        ou valeurSimple peut contenir : +/- (obligatoire/sauf) asteriks/? (serie de caracteres / 1 caractere) AND/OR (surtout le OR car +=AND)
        NB: si asteriks/? => analyze_wildcard : true
    (*)reqwildcard = $wildcard : { nomfield : valeurSimple }
        ou valeurSimple peut contenir : asteriks/? (serie de caracteres / 1 caractere)
    (+)reqregex = $regexp : { field : { $regex : exprReg, $options : o } }
        NB : garde cette possibilite ?
    reqlimit = $limit : number
    
(*) ES only, (+) MD only, (=) variabilité possible

 * }
 * </pre>
 * @author "Frederic Bregier"
 * 
 */
public class QueryBench extends MdEsQueryParser {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(QueryBench.class);
    
    private static final String CPTLEVEL = "__cptlevel__";

    private static final String VARY = "vary";
    private static final String MODEL = "model";
    
    protected static enum FIELD {
		save, liste, listeorder, serie, interval, setdistrib
	}

    protected static enum FIELD_ARGS { 
		__name, __type, __ftype,
		__liste, __listeorder, __serie, __prefix, __idcpt, __modulo, __low, __high, __subprefix, __save
	}
    
    protected static enum FIELD_TYPE {
        chaine, date, nombre, nombrevirgule
    }

    /**
     * Use to implement variability of Field
     * @author "Frederic Bregier"
     *
     */
    protected static class TypeField {
        public String name;
        /**
         * Between liste, listeorder, serie
         */
        public FIELD type;
        /**
         * Type of value (default chaine)
         */
        public FIELD_TYPE ftype = FIELD_TYPE.chaine;
        /**
         * Used in liste, listeorder
         */
        public String [] listeValeurs;
        /**
         * Used in serie for fixed value in constant
         */
        public String prefix;
        public String idcpt;
        public int modulo;
        /**
         * Interval
         */
        public int low;
        /**
         * Interval
         */
        public int high;
        /**
         * saved as new current value for name
         */
        public String saveName;
        /**
         * add several prefix to value
         */
        public String [] subprefixes;
        
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("Field: ");
            builder.append(name);
            builder.append(" Type: "+type);
            builder.append(" FType: "+ftype);
            builder.append(" Prefix: "+prefix);
            builder.append(" Cpt: "+idcpt);
            builder.append(" Modulo: "+modulo);
            builder.append(" Low: "+low);
            builder.append(" High: "+high);
            builder.append(" Save: "+saveName);
            if (subprefixes != null) {
                builder.append(" SubPrefix: [ ");
                for (String curname : subprefixes) {
                    builder.append(curname);
                    builder.append(' ');
                }
                builder.append(']');
            }
            if (listeValeurs != null) {
                builder.append(" Vals: "+listeValeurs.length);
            }
            return builder.toString();
        }
    }
    
	BenchContext context = new BenchContext();
	AtomicLong distribCpt = null;
    List<List<TypeField>> levelFields = new ArrayList<List<TypeField>>();
    List<JsonNode> levelRequests = new ArrayList<JsonNode>();
    String model;
    long cacheRanks = 0;
    long queryCount = 0;
    long cacheCount = 0;
    
	/**
	 * @param simul
	 */
	public QueryBench(boolean simul) {
		super(simul);
	}
	
	/**
	 * Prepare the parse
	 * @param request
	 * @throws InvalidParseOperationException
	 */
	public void prepareParse(String request) throws InvalidParseOperationException {
		final JsonNode rootNode = JsonHandler.getFromString(request);
		if (rootNode.isMissingNode()) {
            throw new InvalidParseOperationException("The current Node is missing(empty): RequestRoot");
        }
		// take model
		JsonNode node = ((ArrayNode) rootNode).remove(0);
		model = node.get(MODEL).asText();
		// level are described as array entries, each being single element (no name)
        int level = 0;
        for (final JsonNode jlevel : rootNode) {
            context.cpts.put(CPTLEVEL+level, new AtomicLong(0));
            // now parse sub element as single command/value
            analyzeVary(jlevel, level);
            level ++;
        }
	}
	/**
	 * 
	 * @return the model associated with the preparedParsed file
	 */
	public String getModel() {
	    return model;
	}
	
	/**
     * { expression, $depth : exactdepth, $relativedepth : /- depth, vary : [] }, 
     * $depth and $relativedepth being optional (mutual exclusive),
     * vary being optional
     *
     * @param command
     * @param level
     * @throws InvalidParseOperationException
     */
	private void analyzeVary(final JsonNode command, int level) throws InvalidParseOperationException {
        if (command == null) {
            throw new InvalidParseOperationException("Not correctly parsed");
        }
        // check vary
        List<TypeField> fields = new ArrayList<>();
        if (command.has(VARY)) {
            final ArrayNode jvary = (ArrayNode) ((ObjectNode) command).remove(VARY);
            for (int i = 0; i < jvary.size(); i++) {
                JsonNode node = jvary.get(i);
                TypeField tf = getField(node, level);
                if (tf != null) {
                    fields.add(tf);
                }
            }
        }
        levelFields.add(fields);
        levelRequests.add(command);
    }
	
	private TypeField getField(JsonNode bfield, int level) throws InvalidParseOperationException {
		String name = bfield.get(FIELD_ARGS.__name.name()).asText();
		String type = bfield.get(FIELD_ARGS.__type.name()).asText();
        String sftype = bfield.path(FIELD_ARGS.__ftype.name()).asText();
		if (type == null || type.isEmpty()) {
			LOGGER.warn("Unknown empty type: {}", type);
			throw new InvalidParseOperationException("Unknown empty type: "+type);
		}
		TypeField field = new TypeField();
		FIELD fieldType = null;
		try {
			fieldType = FIELD.valueOf(type);
		} catch (IllegalArgumentException e) {
		    LOGGER.warn("Unknown type: {}", bfield);
			throw new InvalidParseOperationException("Unknown type: "+bfield);
		}
		field.name = name;
		field.type = fieldType;
		FIELD_TYPE ftype = FIELD_TYPE.chaine;
        if (sftype != null && ! sftype.isEmpty()) {
            try {
                ftype = FIELD_TYPE.valueOf(sftype);
            } catch (final IllegalArgumentException e) {
                LOGGER.error("Unknown ftype: " + bfield);
                ftype = FIELD_TYPE.chaine;
            }
        }
        field.ftype = ftype;
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
			    ArrayNode liste = (ArrayNode) bfield.get("__"+fieldType.name());
				if (liste == null || ! liste.has(0)) {
				    LOGGER.warn("Empty List: {}", liste);
					throw new InvalidParseOperationException("Empty List: "+bfield);
				}
				field.listeValeurs = new String[liste.size()];
				for (int i = 0; i < liste.size(); i++) {
					field.listeValeurs[i] = liste.get(i).asText();
				}
				break;
			}
			case serie: {
				JsonNode bson = bfield.get(FIELD_ARGS.__serie.name());
				if (bson == null) {
				    LOGGER.warn("Empty serie: {}", bfield);
					throw new InvalidParseOperationException("Empty serie: "+bfield);
				}
				if (bson.has(FIELD_ARGS.__prefix.name())) {
					String prefix = bson.get(FIELD_ARGS.__prefix.name()).asText();
					if (prefix == null) {
						prefix = "";
					}
					field.prefix = prefix;
				}
				if (bson.has(FIELD_ARGS.__idcpt.name())) {
					String idcpt = bson.get(FIELD_ARGS.__idcpt.name()).asText();
					if (idcpt != null && ! idcpt.isEmpty()) {
						field.idcpt = idcpt;
						context.cpts.put(idcpt, new AtomicLong(0));
					}
				}
				field.modulo = -1;
				if (bson.has(FIELD_ARGS.__modulo.name())) {
					int modulo = bson.get(FIELD_ARGS.__modulo.name()).asInt();
					if (modulo > 0) {
						field.modulo = modulo;
					}
				}
				break;
			}
			case interval: {
				Integer low = bfield.get(FIELD_ARGS.__low.name()).asInt();
				if (low == null) {
				    LOGGER.warn("Empty interval: {}", bfield);
					throw new InvalidParseOperationException("Empty interval: "+bfield);
				}
				Integer high = bfield.get(FIELD_ARGS.__high.name()).asInt();
				if (high == null) {
				    LOGGER.warn("Empty interval: {}", bfield);
					throw new InvalidParseOperationException("Empty interval: "+bfield);
				}
				field.low = low;
				field.high = high;
				break;
			}
			default:
			    LOGGER.warn("Incorrect type: {}", bfield);
				throw new InvalidParseOperationException("Incorrect type: "+bfield);
		}
		if (bfield.has(FIELD_ARGS.__save.name())) {
			String savename = bfield.get(FIELD_ARGS.__save.name()).asText();
			if (savename != null && ! savename.isEmpty()) {
				field.saveName = savename;
			}
		}
		if (bfield.has(FIELD_ARGS.__subprefix.name())) {
			ArrayNode liste = (ArrayNode) bfield.get(FIELD_ARGS.__subprefix.name());
			if (liste == null || ! liste.has(0)) {
			    LOGGER.warn("Empty SubPrefix List: {}", liste);
				throw new InvalidParseOperationException("Empty SubPrefix List: "+bfield);
			}
			field.subprefixes = new String[liste.size()];
			for (int i = 0; i < liste.size(); i++) {
				field.subprefixes[i] = liste.get(i).asText();
			}
		}
		return field;
	}
	
	/**
	 * To be called each time an execute will be called if the request starts from zero
	 * @param indexName ES Index
	 * @param typeName ES type in Index
	 * @return the new BenchContext
	 */
	public BenchContext getNewContext(String indexName, String typeName) {
		BenchContext newBenchContext = new BenchContext();
		newBenchContext.indexName = indexName;
		newBenchContext.typeName = typeName;
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
	
	/**
	 * Execute the benchmark against the prepared parsed requests
	 * @param dbvitam
	 * @param start
	 * @param newBenchContext
	 * @return the list of Result (level by level), to be used in finalizeResults method
	 * @throws InvalidExecOperationException
	 * @throws InvalidParseOperationException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public List<ResultInterface> executeBenchmark(MongoDbAccess dbvitam, long start, BenchContext newBenchContext) throws InvalidExecOperationException, InvalidParseOperationException, InstantiationException, IllegalAccessException {
        QueryBench executeParser= new QueryBench(simulate);
        JsonNode empty = JsonHandler.createObjectNode();
        executeParser.filterParse(empty);
        executeParser.projectionParse(empty);
        int level = 0;
        if (newBenchContext.distrib != null) {
            newBenchContext.distrib.set(start);
        }
        AtomicLong rank = newBenchContext.cpts.get(CPTLEVEL+level);
        ArrayNode array = JsonHandler.createArrayNode();
        // Build the executeParser
        for (int i = 0; i < levelRequests.size(); i++) {
            String request = getRequest(levelRequests.get(i), levelFields.get(i), rank, newBenchContext);
            if (request != null) {
                array.add(JsonHandler.getFromString(request));
            } else {
                array.add(levelRequests.get(i).deepCopy());
            }
            level++;
            rank = newBenchContext.cpts.get(CPTLEVEL+level);
        }
        executeParser.queryParse(array);
        LOGGER.debug("Will execute: {}\n\t{}", executeParser.getSources(), executeParser.getRequests());
        // Now execute
        DbRequest dbrequest = new DbRequest(dbvitam, newBenchContext.indexName, newBenchContext.typeName);
        dbrequest.setSimulate(simulate);
        dbrequest.setUseCache(GlobalDatas.SAVERESULT);
        ResultInterface startSet = MongoDbAccess.createOneResult(GlobalDatas.ROOTS);
        startSet.setMaxLevel(0);
        startSet.setMinLevel(0);
        startSet.putBeforeSave();
        List<ResultInterface> list = dbrequest.execQuery(executeParser, startSet);
        cacheRanks += dbrequest.getLastCacheRank();
        cacheCount += dbrequest.getLastCacheQueryCount();
        queryCount += dbrequest.getLastRealExecutedQueryCount();
        return list;
	}
	
	/**
	 * @param dbvitam
	 * @param newBenchContext
	 * @param results
	 * @return the final ResultCached
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public ResultInterface finalizeResults(MongoDbAccess dbvitam, BenchContext newBenchContext, List<ResultInterface> results) throws InstantiationException, IllegalAccessException {
	    DbRequest dbrequest = new DbRequest(dbvitam, newBenchContext.indexName, newBenchContext.typeName);
        dbrequest.setSimulate(simulate);
        dbrequest.setUseCache(GlobalDatas.SAVERESULT);
        return dbrequest.finalizeResults(false, results);
	}
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
        for (int i = 0; i < levelRequests.size(); i++) {
            builder.append("\n");
            for (TypeField type : levelFields.get(i)) {
                builder.append(type);
                builder.append(",");
            }
            builder.append("\n\t");
            builder.append(levelRequests.get(i));
		}
		return builder.toString();
	}

    private static final String getValue(TypeField typeField, String curval, Map<String, String> savedNames) {
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
    
    private static final String getFinalRequest(TypeField typeField, String curval, Map<String, String> savedNames, String request) {
        final String finalVal = getValue(typeField, curval, savedNames);
        if (typeField.ftype == FIELD_TYPE.nombre || typeField.ftype == FIELD_TYPE.nombrevirgule) {
            return request.replace('\"'+typeField.name+'\"', finalVal);  
        } else {
            return request.replace(typeField.name, finalVal);
        }
        
    }
    
    private static String getRequest(JsonNode request, List<TypeField> fields, AtomicLong rank, BenchContext bench) {
        if (fields != null && ! fields.isEmpty()) {
            String finalRequest = request.toString();
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            for (TypeField field : fields) {
                String val = null;
                switch (field.type) {
                    case save:
                        finalRequest = getFinalRequest(field, "", bench.savedNames, finalRequest);
                        break;
                    case liste:
                        int rlist = rnd.nextInt(field.listeValeurs.length);
                        val = field.listeValeurs[rlist];
                        finalRequest = getFinalRequest(field, val, bench.savedNames, finalRequest);
                        break;
                    case listeorder:
                        long i = rank.getAndIncrement();
                        if (i >= field.listeValeurs.length) {
                            i = field.listeValeurs.length-1;
                        }
                        val = field.listeValeurs[(int) i];
                        finalRequest = getFinalRequest(field, val, bench.savedNames, finalRequest);
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
                        finalRequest = getFinalRequest(field, val, bench.savedNames, finalRequest);
                        break;
                    case interval:
                        int newval = rnd.nextInt(field.low, field.high+1);
                        finalRequest = getFinalRequest(field, ""+newval, bench.savedNames, finalRequest);
                        break;
                    default:
                        break;
                }
            }
            return finalRequest;
        }
        return null;
    }
}
