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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.BSONObject;
import org.bson.types.BasicBSONList;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;

import fr.gouv.vitam.mdbes.DAip;
import fr.gouv.vitam.mdbes.Domain;
import fr.gouv.vitam.mdbes.DuaRef;
import fr.gouv.vitam.mdbes.MongoDbAccess;
import fr.gouv.vitam.mdbes.MongoDbAccess.VitamCollections;
import fr.gouv.vitam.mdbes.PAip;
import fr.gouv.vitam.query.exception.InvalidExecOperationException;
import fr.gouv.vitam.query.exception.InvalidParseOperationException;
import fr.gouv.vitam.query.exception.InvalidUuidOperationException;
import fr.gouv.vitam.utils.FileUtil;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * @author "Frederic Bregier"
 * 
 */
public class ParserIngest {
	private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(ParserIngest.class);
	
	protected BufferedOutputStream bufferedOutputStream = null;
	private static final Map<String, DAip> DAIP_BELOW_MINLEVEL = new HashMap<String, DAip>();
	private static final String CPTLEVEL = "__cptlevel__";
	private static final String REFID = "_refid";
	private static enum OCCURENCE_ARGS { 
		__occur, __idcpt, __high, __distrib, __notempty
	}

	private static enum FIELD { 
		chaine, date, nombre, save, constant, constantArray, liste, listeorder, serie, subfield, interval, select
	}

	private static enum FIELD_ARGS { 
		__model, __idcpt, 
		__type, __liste, __listeorder, __serie, __prefix, __modulo, __subprefix, __low, __high, __field, __value,
		__save, __subfield
	}

	private static class Occurence {
		private int occur;
		private int low;
		private int high;
		private int distrib;
		private boolean notempty = false;
		private String idcpt;
	}

	private static class TypeField {
		private String name;
		/**
		 * Between chaine, date, nombre, liste, listeorder, serie, subfield, constant, constantArray
		 */
		private FIELD type;
		/**
		 * Used in liste, listeorder, constantArray
		 */
		private String [] listeValeurs;
		/**
		 * Used in serie and prefix for fixed value in constant
		 */
		private String prefix;
		private String idcpt;
		private int modulo;
		/**
		 * Interval
		 */
		private int low;
		private int high;
		/**
		 * saved as new current value for name
		 */
		private String saveName;
		/**
		 * add several prefix to value
		 */
		private String [] subprefixes;
		/**
		 * Used in subfield
		 */
		private List<TypeField> subfields;
		
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("\nField: ");
			builder.append(name);
			builder.append(" Type: "+type);
			builder.append(" Prefix: "+prefix);
			builder.append(" Cpt: "+idcpt);
			builder.append(" Modulo: "+modulo);
			builder.append(" High: "+high);
			builder.append(" Save: "+saveName);
			if (listeValeurs != null) {
				builder.append(" Vals: "+listeValeurs.length);
			}
			builder.append(" Save: "+saveName);
			if (subprefixes != null) {
				builder.append(" SubPrefix: [ ");
				for (String curname : subprefixes) {
					builder.append(curname);
					builder.append(' ');
				}
				builder.append(']');
			}
			if (subfields != null) {
				builder.append(" Subfields: "+subfields.size());
				builder.append("\n\t[");
				for (TypeField typeSubField : subfields) {
					builder.append(typeSubField.toString());
				}
				builder.append("\n\t]");
			}
			return builder.toString();
		}
	}

	
	private String ingest;
	private MongoDbAccess dbvitam;
	private String domRefid;
	private BSONObject domObj;
	private Domain domobj;
	private String model;
	private Occurence distribOccurence = null;
	
	private BenchContext context = new BenchContext();
	private ArrayList<Occurence> occurences = new ArrayList<>();
	/**
	 * Model of DAIPs from model
	 */
	private ArrayList<List<TypeField>> daips = new ArrayList<>();
	/**
	 * Model of PAip from model
	 */
	private List<TypeField> paips = null;
	
	private AtomicLong totalCount = new AtomicLong(0);
	
	private boolean simulate = false;
	
	public ParserIngest(boolean simul) throws InvalidParseOperationException {
		setSimulate(simul);
	}
	
	/**
	 * @return the totalCount
	 */
	public long getTotalCount() {
		return totalCount.get();
	}

	public void setSimulate(boolean simul) {
		this.simulate = simul;
	}
	
	private void internalParseClean() {
		context.cpts.clear();
		context.savedNames.clear();
		occurences.clear();
		daips.clear();
		if (paips != null) {
			paips.clear();
		}
	}
	/**
	 * Parse the model of ingest from the string
	 * @param ingest
	 * @throws InvalidParseOperationException
	 */
	public void parse(String ingest) throws InvalidParseOperationException {
		internalParseClean();
		this.ingest = ingest;
		BSONObject bson = (BSONObject) JSON.parse(ingest);
		this.model = (String) bson.removeField(FIELD_ARGS.__model.name());
		getDomain(bson);
		parseDAip(bson, 1);
		domObj = bson;
	}
	
	public String getModel() {
		return this.model;
	}
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Model: ");
		builder.append(model);
		builder.append(" Domaine: ");
		builder.append(domRefid);
		if (domObj != null && ! domObj.toMap().isEmpty()) {
			builder.append(" other: "+domObj.toString());
		}
		int nb = occurences.size();
		for (int i = 0; i < nb ; i++) {
			builder.append("\nMaipLevel: ");
			builder.append((i+1));
			Occurence occur = occurences.get(i);
			builder.append(" Occurence: ");
			builder.append(occur.occur+":"+occur.idcpt+":"+occur.low+":"+occur.high+":"+occur.notempty);
			List<TypeField> fields = daips.get(i);
			for (TypeField typeField : fields) {
				builder.append(typeField.toString());
			}
		}
		builder.append("\nDataLevel: ");
		builder.append((nb+1));
		if (paips != null) {
			for (TypeField typeField : paips) {
				builder.append(typeField.toString());
			}
		}
		return builder.toString();
	}
	/**
	 * Set the domain Refid from model
	 * @param bson
	 * @throws InvalidParseOperationException
	 */
	private void getDomain(BSONObject bson) throws InvalidParseOperationException {
		// Find Domaine as reference (and possibly create later)
		domRefid = (String) bson.removeField(MongoDbAccess.VitamCollections.Cdomain.getName());
	}
	/**
	 * Parse the bson model from level
	 * @param bson
	 * @param level current level
	 * @throws InvalidParseOperationException
	 */
	private void parseDAip(BSONObject bson, int level) throws InvalidParseOperationException {
		if (bson.containsField(MongoDbAccess.VitamCollections.Cdaip.getName())) {
			BasicBSONList metaaips = (BasicBSONList) bson.removeField(MongoDbAccess.VitamCollections.Cdaip.getName());
			// should be 0 = occurence, 1 = fields, 2 = sub-structure (maip or dataobject)
			BSONObject occur = (BSONObject) metaaips.get(0);
			BSONObject maipModel = (BSONObject) metaaips.get(1);
			BSONObject substruct = (BSONObject) metaaips.get(2);
			int nb = (int) occur.get(OCCURENCE_ARGS.__occur.name());
			Occurence oc = new Occurence();
			oc.occur = nb;
			if (occur.containsField(OCCURENCE_ARGS.__high.name())) {
				oc.high = (Integer) occur.get(OCCURENCE_ARGS.__high.name());
				oc.low = nb;
				oc.occur = oc.high- oc.low + 1;
				oc.distrib = oc.low;
			} else {
				oc.low = 0;
				oc.high = nb-1;
				oc.distrib = oc.low;
			}
			if (occur.containsField(OCCURENCE_ARGS.__distrib.name())) {
				oc.distrib = (Integer) occur.get(OCCURENCE_ARGS.__distrib.name());
				distribOccurence = oc;
				System.out.println("Found a distribution occurence");
			}
			if (occur.containsField(OCCURENCE_ARGS.__idcpt.name())) {
				oc.idcpt = (String) occur.get(OCCURENCE_ARGS.__idcpt.name());
				context.cpts.put(oc.idcpt, new AtomicLong(oc.low));
			}
			if (occur.containsField(OCCURENCE_ARGS.__notempty.name())) {
				oc.notempty = true;
			}
			context.cpts.put(CPTLEVEL+level, new AtomicLong(oc.low));
			occurences.add(oc);
			// now parse MetaAip field
			List<TypeField> fields = new ArrayList<>();
			System.out.println("LoadM: "+level+" "+maipModel);
			Set<String> fieldnames = maipModel.keySet();
			for (String fieldname : fieldnames) {
				Object bfield = maipModel.get(fieldname);
				if (bfield instanceof String) {
					// single value
					TypeField field = new TypeField();
					field.name = fieldname;
					field.prefix = (String) bfield;
					field.type = FIELD.constant;
					fields.add(field);
				} else if (bfield instanceof BasicBSONList) {
					// single value
					BasicBSONList blist = (BasicBSONList) bfield;
					TypeField field = new TypeField();
					field.name = fieldname;
					field.listeValeurs = new String[blist.size()];
					for (int j = 0; j < blist.size(); j++) {
						field.listeValeurs[j] = (String) blist.get(j);
					}
					field.type = FIELD.constantArray;
					fields.add(field);
				} else {
					TypeField field = getField((BSONObject) bfield);
					field.name = fieldname;
					fields.add(field);
				}
			}
			daips.add(fields);
			parseDAip(substruct, level+1);
		} else if (bson.containsField(MongoDbAccess.VitamCollections.Cpaip.getName())) {
			parsePAip(bson, level);
		}
	}
	/**
	 * 
	 * @param bfield
	 * @return one TypeField according to bson element
	 * @throws InvalidParseOperationException
	 */
	private TypeField getField(BSONObject bfield) throws InvalidParseOperationException {
		String type = (String) bfield.get(FIELD_ARGS.__type.name());
		if (type == null || type.isEmpty()) {
			System.err.println("Unknown empty type: "+type);
			throw new InvalidParseOperationException("Unknown empty type: "+type);
		}
		TypeField field = new TypeField();
		FIELD fieldType = null;
		try {
			fieldType = FIELD.valueOf(type);
		} catch (IllegalArgumentException e) {
			System.err.println("Unknown type: "+bfield);
			throw new InvalidParseOperationException("Unknown type: "+bfield);
		}
		field.type = fieldType;
		switch (fieldType) {
			case chaine:
			case date:
			case nombre:
			case save:
				break;
			case liste:
			case listeorder: {
				BasicBSONList liste = (BasicBSONList) bfield.get("__"+fieldType.name());
				if (liste == null || liste.isEmpty()) {
					System.err.println("Empty List: "+liste);
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
					System.err.println("Empty serie: "+bfield);
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
			case subfield: {
				BSONObject subfield = (BSONObject) bfield.get(FIELD_ARGS.__subfield.name());
				if (subfield == null) {
					System.err.println("Unknown subfield: "+bfield);
					throw new InvalidParseOperationException("Unknown subfield: "+bfield);
				}
				List<TypeField> fields = new ArrayList<>();
				System.out.println("LoadMS: "+subfield);
				Set<String> fieldnames = subfield.keySet();
				for (String fieldname : fieldnames) {
					BSONObject bfield2 = (BSONObject) subfield.get(fieldname);
					TypeField field2 = getField(bfield2);
					field2.name = fieldname;
					fields.add(field2);
				}
				field.subfields = fields;
				break;
			}
			case interval: {
				Integer low = (Integer) bfield.get(FIELD_ARGS.__low.name());
				if (low == null) {
					System.err.println("Empty interval: "+bfield);
					throw new InvalidParseOperationException("Empty interval: "+bfield);
				}
				Integer high = (Integer) bfield.get(FIELD_ARGS.__high.name());
				if (high == null) {
					System.err.println("Empty interval: "+bfield);
					throw new InvalidParseOperationException("Empty interval: "+bfield);
				}
				field.low = low;
				field.high = high;
				break;
			}
			case select: {
				BasicBSONList liste = (BasicBSONList) bfield.get("__"+fieldType.name());
				if (liste == null || liste.isEmpty()) {
					System.err.println("Empty List: "+liste);
					throw new InvalidParseOperationException("Empty List: "+bfield);
				}
				field.subfields = new ArrayList<>();
				for (int i = 0; i < liste.size(); i++) {
					TypeField request = new TypeField();
					BSONObject obj = (BSONObject) liste.get(i);
					request.name = (String) obj.get(FIELD_ARGS.__field.name());
					BasicBSONList prefixes = (BasicBSONList) obj.get(FIELD_ARGS.__value.name());
					if (prefixes == null || prefixes.isEmpty()) {
						System.err.println("Empty prefixes: "+prefixes);
						throw new InvalidParseOperationException("Empty prefixes: "+obj);
					}
					request.subprefixes = new String[prefixes.size()];
					for (int j = 0; j < prefixes.size(); j++) {
						request.subprefixes[j] = (String) prefixes.get(j);
					}
					field.subfields.add(request);
				}
				break;
			}
			default:
				System.err.println("Incorrect type: "+bfield);
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
	/**
	 * Parse the final PAip
	 * @param bson
	 * @param level
	 * @throws InvalidParseOperationException
	 */
	private void parsePAip(BSONObject bson, int level) throws InvalidParseOperationException {
		if (bson.containsField(MongoDbAccess.VitamCollections.Cpaip.getName())) {
			BSONObject bdata = (BSONObject) bson.removeField(MongoDbAccess.VitamCollections.Cpaip.getName());
			context.cpts.put(CPTLEVEL+level, new AtomicLong(0));
			List<TypeField> fields = new ArrayList<>();
			System.out.println("LoadD: "+level+" "+bdata);
			Set<String> fieldnames = bdata.keySet();
			for (String fieldname : fieldnames) {
				Object bfield = bdata.get(fieldname);
				if (bfield instanceof String) {
					// single value
					TypeField field = new TypeField();
					field.name = fieldname;
					field.prefix = (String) bfield;
					field.type = FIELD.constant;
					fields.add(field);
				} else if (bfield instanceof BasicBSONList) {
					// single value
					BasicBSONList blist = (BasicBSONList) bfield;
					TypeField field = new TypeField();
					field.name = fieldname;
					field.listeValeurs = new String[blist.size()];
					for (int j = 0; j < blist.size(); j++) {
						field.listeValeurs[j] = (String) blist.get(j);
					}
					field.type = FIELD.constantArray;
					fields.add(field);
				} else {
					TypeField field = getField((BSONObject) bfield);
					field.name = fieldname;
					fields.add(field);
				}
			}
			paips = fields;
		}
	}
	/**
	 * Save generated DAip to the file and to ElasticSearch
	 * @param dbvitam
	 * @param start
	 * @param stop
	 * @return
	 * @throws InvalidExecOperationException
	 * @throws InvalidUuidOperationException
	 */
	public long executeToFile(MongoDbAccess dbvitam, int start, int stop) throws InvalidExecOperationException, InvalidUuidOperationException {
		if (simulate) {
			return executeSimulate(start, stop);
		}
		System.out.println("Start To File");
		this.dbvitam = dbvitam;
		// Domain
		domobj = (Domain) dbvitam.fineOne(VitamCollections.Cdomain, REFID, domRefid);
		LOGGER.warn("Found Domain ? "+(domobj != null));
		if (domobj == null) {
			domobj = new Domain();
			domobj.put(REFID, domRefid);
			domobj.put("name", domRefid);
			domobj.putAll(domObj);
			domobj.save(dbvitam);
			domobj.setRoot();
			LOGGER.warn("Create Domain: {}", domobj);
			//System.err.println("Load: "+domobj);
		}
		// Set DISTRIB to start-stop
		if (distribOccurence != null) {
			int lstop = (stop-start+1 > distribOccurence.occur) ? 
					start + distribOccurence.occur -1 : stop;
			distribOccurence.low = start;
			distribOccurence.high = lstop;
			distribOccurence.occur = lstop - start +1;
			System.out.println("Distrib: "+start+":"+lstop);
		}
		// First level using start-stop
		AtomicLong cpt = context.cpts.get(CPTLEVEL+1);
		List<TypeField> fields = daips.get(0);
		Occurence occurence = occurences.get(0);
		cpt.set(occurence.low);
		if (occurence.idcpt != null) {
			cpt = context.cpts.get(occurence.idcpt);
		}
		long lstart = cpt.get();
		long lstop = lstart+occurence.occur-1;
		ArrayList<DAip> listmetaaips;
		HashMap<String, Integer> subdepth = new HashMap<>();
		subdepth.put(domobj.getId(), 1);
		HashMap<String, String> esIndex = new HashMap<>();
		try {
			listmetaaips = execDAipNewToFile(null, subdepth, esIndex, 1, occurence, lstart, lstop, cpt, fields);
		} catch (InstantiationException | IllegalAccessException e) {
			throw new InvalidExecOperationException(e);
		}
		System.out.println("End of MAIPs");
		if (listmetaaips != null && ! listmetaaips.isEmpty()) {
			if (MainIngestFile.minleveltofile > 1) {
				domobj.addMetaAip(dbvitam, listmetaaips);
			} else {
				// XXX NO SAVE OF MAIP!
				domobj.addMetaAipNoSave(dbvitam, bufferedOutputStream, listmetaaips);
			}
			domobj.save(dbvitam);
			listmetaaips.clear();
			listmetaaips = null;
		}
		if (! esIndex.isEmpty()) {
			System.out.println("Last bulk ES");
			dbvitam.addEsEntryIndex(true, esIndex, model);
			esIndex.clear();
		}
		System.out.println("End of Domain");
		return totalCount.get();
	}

	/**
	 * Level 1 to MD, not > 1
	 * 
	 * Save to file and to ElasticSearch
	 * @param father
	 * @param subdepth22
	 * @param esIndex
	 * @param level
	 * @param occurence
	 * @param lstart
	 * @param lstop
	 * @param cpt
	 * @param fields
	 * @return
	 * @throws InvalidExecOperationException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private ArrayList<DAip> execDAipNewToFile(DAip father, Map<String, Integer> subdepth22, HashMap<String, String> esIndex, int level, Occurence occurence, long lstart, long lstop, AtomicLong cpt, List<TypeField> fields) throws InvalidExecOperationException, InstantiationException, IllegalAccessException {
		ArrayList<DAip> listmetaaips = new ArrayList<DAip>();
		for (long rank = cpt.get(); rank <= lstop; rank = cpt.incrementAndGet()) {
			DAip maip = new DAip();
			maip.put(DAip.DAIPDEPTHS, subdepth22);
			for (TypeField typeField : fields) {
				BasicDBObject obj = getDbObject(typeField, rank, occurence.distrib, cpt);
				if (obj == null) {
					maip = null;
					break;
				}
				maip.putAll((BSONObject) obj);
			}
			if (maip == null) {
				continue;
			}
			totalCount.incrementAndGet();
			if (totalCount.get()%1000 == 0) {
				System.out.print('.');
			}
			if (occurence == distribOccurence) {
				System.out.println("\nDistrib: "+rank);
			}
			maip.getAfterLoad();
			DAip metaaip2 = null;
			if (level < MainIngestFile.minleveltofile) {
				metaaip2 = (DAip) dbvitam.fineOne(VitamCollections.Cdaip, REFID, maip.getString(REFID));
				if (metaaip2 != null) {
					// already existing so take this one
					DAIP_BELOW_MINLEVEL.put(metaaip2.getString(REFID), metaaip2);
				}
			}
			metaaip2 = DAIP_BELOW_MINLEVEL.get(maip.getString(REFID));
			boolean metaCreated = true;
			if (metaaip2 != null) {
				// merge Depth
				Map<String, Integer> old = metaaip2.getDomDepth();
				//Map<String, Integer> toUpdateSon = new HashMap<String, Integer>();
				for (String key : subdepth22.keySet()) {
					if (old.containsKey(key)) {
						if (old.get(key) > subdepth22.get(key)){
							old.put(key, subdepth22.get(key));
							//toUpdateSon.put(key, subdepth22.get(key));
						}
					} else {
						old.put(key, subdepth22.get(key));
						//toUpdateSon.put(key, subdepth22.get(key));
					}
				}
				// old now contains all
				metaaip2.put(DAip.DAIPDEPTHS, old);
				// XXX FIXME should update children but will not since "POC" code and not final code
				// XXX FIXME here should do: recursive call to update DAIPDEPTHS from toUpdateSon
				maip = metaaip2;
				//System.out.println("Not created: "+metaaip2.toString());
				metaCreated = false;
				if (level >= daips.size()) {
					// update directly
					if (father == null) {
						listmetaaips.add(metaaip2);
					}
					int nbEs = metaaip2.addEsIndex(dbvitam, esIndex, model);
					//MainIngestFile.cptMaip.addAndGet(nbEs);
			        if (level < MainIngestFile.minleveltofile) {
			            metaaip2.save(dbvitam);
			        } else {
			        	metaaip2.saveToFile(dbvitam, bufferedOutputStream, level);
			        }
					continue;
				}
			}
			if (metaCreated) {
				// since created saved once here
				// XXX NO SAVE OF MAIP!
				maip.setNewId();
				// now check duaref and confidentialLevel
				if (maip.containsField(MongoDbAccess.VitamCollections.Cdua.getName())) {
					String duaname =  (String) maip.removeField(MongoDbAccess.VitamCollections.Cdua.getName());
					DuaRef duaobj = DuaRef.findOne(dbvitam, duaname);
					if (duaobj != null) {
						maip.addDuaRef(dbvitam, duaobj);
					} else {
						System.err.println("wrong dua: "+duaname);
					}
				}
			}
			// now compute subMaip if any
			if (level < daips.size()) {
				AtomicLong newcpt = context.cpts.get(CPTLEVEL+(level+1));
				List<TypeField> newfields = daips.get(level);
				Occurence occurence2 = occurences.get(level);
				// default reset
				newcpt.set(occurence2.low);
				if (occurence2.idcpt != null) {
					newcpt = context.cpts.get(occurence2.idcpt);
				}
				long newlstart = newcpt.get();
				long newlstop = newlstart+occurence2.occur-1;
				Map<String, Integer> subdepth2 = maip.getSubDomDepth();
				ArrayList<DAip> listsubmetaaips = 
						execDAipNewToFile(maip, subdepth2, esIndex, level+1, occurence2, newlstart, newlstop, newcpt, newfields);
				if (listsubmetaaips != null) {
					listsubmetaaips.clear();
					listsubmetaaips = null;
				}
				/*if (listsubmetaaips != null && ! listsubmetaaips.isEmpty()) {
					maip.addMetaAip(dbvitam, listsubmetaaips);
					listsubmetaaips.clear();
				} else if (occurence2.notempty && metaCreated) {
					// should not be empty so delete it if created
					maip.delete(dbvitam.metaaips);
					//System.out.println("removed: "+maip.refid);
					continue;
				}
				listsubmetaaips = null;
				*/
			} else {
				// now check data
				if (paips != null) {
					// ignore ? XXX FIXME
					/*DataObject dataobj = new DataObject();
					for (TypeField typeField : dataObject) {
						BasicDBObject obj = getDbObject(typeField, rank, occurence.distrib, cpt);
						dataobj.putAll((BSONObject) obj);
					}
					dataobj.setRefid(maip.refid);
					dataobj.getAfterLoad();
					dataobj.save(dbvitam);
					maip.addDataObject(dbvitam, dataobj);*/
				}
			}
			if (father != null) {
				father.addDAipWithNoSave(maip);
			}
			// XXX NO SAVE OF MAIP!
			if (level < MainIngestFile.minleveltofile) {
				DAIP_BELOW_MINLEVEL.put(maip.getString(REFID), maip);
			}
			//System.out.println("M: "+maip.toString());
			int nbEs = maip.addEsIndex(dbvitam, esIndex, model);
			//MainIngestFile.cptMaip.addAndGet(nbEs);
			if (metaCreated && father == null) {
				listmetaaips.add(maip);
			} else if (father != null) {
		        if (level < MainIngestFile.minleveltofile) {
		            maip.save(dbvitam);
		        } else {
		        	maip.saveToFile(dbvitam, bufferedOutputStream, level);
		        }
			}
		}
		return listmetaaips;
	}

	/**
	 * Execute in simulate mode
	 * @param start
	 * @param stop
	 * @return the number of simulated node
	 * @throws InvalidExecOperationException
	 */
	public long executeSimulate(int start, int stop) throws InvalidExecOperationException {
		this.simulate = true;
		// Domain
		System.out.println("Domain: "+domRefid);
		// Set DISTRIB to start-stop
		if (distribOccurence != null) {
			System.out.println("Update a distribution occurence from: "+start+":"+stop);
			int lstop = (stop-start+1 > distribOccurence.occur) ? 
					start + distribOccurence.occur -1 : stop;
			distribOccurence.low = start;
			distribOccurence.high = lstop;
			distribOccurence.occur = lstop - start +1;
		} else {
			System.out.println("No Found distribution occurence");
		}
		// First level using start-stop
		AtomicLong cpt = context.cpts.get(CPTLEVEL+1);
		List<TypeField> fields = daips.get(0);
		Occurence occurence = occurences.get(0);
		cpt.set(occurence.low);
		System.out.println("CPT: "+(CPTLEVEL+1)+" "+cpt.get());
		if (occurence.idcpt != null) {
			cpt = context.cpts.get(occurence.idcpt);
			System.out.println("CPT replaced by: "+occurence.idcpt);
		}
		long lstart = cpt.get();
		long lstop = lstart+occurence.occur-1;
		ArrayList<DAip> listmetaaips;
		try {
			listmetaaips = execSimulDAip(1, occurence.distrib, lstart, lstop, cpt, fields);
			System.out.println("SubMaips: "+listmetaaips.size());
		} catch (InstantiationException | IllegalAccessException e) {
			throw new InvalidExecOperationException(e);
		}
		return totalCount.get();
	}
	/**
	 * Submethod in simulation mode
	 * @param level
	 * @param base
	 * @param lstart
	 * @param lstop
	 * @param cpt
	 * @param fields
	 * @return the sublist of DAips
	 * @throws InvalidExecOperationException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private ArrayList<DAip> execSimulDAip(int level, int base, long lstart, long lstop, AtomicLong cpt, List<TypeField> fields) throws InvalidExecOperationException, InstantiationException, IllegalAccessException {
		ArrayList<DAip> listmetaaips = new ArrayList<DAip>();
		for (long rank = cpt.get(); rank <= lstop; rank = cpt.incrementAndGet()) {
			DAip maip = new DAip();
			for (TypeField typeField : fields) {
				BasicDBObject obj = getDbObject(typeField, rank, base, cpt);
				if (obj == null) {
					maip = null;
					break;
				}
				maip.putAll((BSONObject) obj);
			}
			if (maip == null) {
				continue;
			}
			totalCount.incrementAndGet();
			maip.getAfterLoad();
			boolean metaCreated = true;
			if (metaCreated) {
				// now check duaref and confidentialLevel
				if (maip.containsField(MongoDbAccess.VitamCollections.Cdua.getName())) {
					maip.get(MongoDbAccess.VitamCollections.Cdua.getName());
				}
			}
			// now compute subMaip if any
			if (level < daips.size()) {
				AtomicLong newcpt = context.cpts.get(CPTLEVEL+(level+1));
				System.out.println("CPT: "+CPTLEVEL+(level+1));
				List<TypeField> newfields = daips.get(level);
				Occurence occurence = occurences.get(level);
				// default reset
				newcpt.set(occurence.low);
				if (occurence.idcpt != null) {
					newcpt = context.cpts.get(occurence.idcpt);
					System.out.println("CPT replaced by: "+occurence.idcpt);
				}
				long newlstart = newcpt.get();
				long newlstop = newlstart+occurence.occur-1;
				ArrayList<DAip> listsubmetaaips = execSimulDAip(level+1, occurence.distrib, newlstart, newlstop, newcpt, newfields);
				System.out.println("SubMaips: "+listsubmetaaips.size());
				listsubmetaaips.clear();
				listsubmetaaips = null;
			} else {
				// now check data
				if (paips != null) {
					PAip dataobj = new PAip();
					for (TypeField typeField : paips) {
						BasicDBObject obj = getDbObject(typeField, rank, base, cpt);
						dataobj.putAll((BSONObject) obj);
					}
					dataobj.put(REFID, maip.get(REFID));
					dataobj.getAfterLoad();
					System.out.println("New Data: "+level+" "+dataobj.toString());
				}
			}
			System.out.println("New Maip: "+level+" "+maip.toString());
			if (metaCreated) {
				listmetaaips.add(maip);
			}
		}
		return listmetaaips;
	}

	/**
	 * 
	 * @param typeField
	 * @param curval
	 * @return a value for the current field
	 */
	private String getValue(TypeField typeField, String curval) {
		String finalVal = curval;
		if (typeField.subprefixes != null) {
			String prefix = "";
			for (String name : typeField.subprefixes) {
				String tempval = context.savedNames.get(name);
				if (tempval != null) {
					prefix += tempval;
				} else {
					prefix += name;
				}
			}
			finalVal = prefix + finalVal;
		}
		if (typeField.saveName != null && finalVal != null) {
			context.savedNames.put(typeField.saveName, finalVal);
		}
		return finalVal;
	}
	/**
	 * 
	 * @param typeField
	 * @param rank
	 * @param lstart
	 * @param cpt
	 * @return the equivalent BSon object for the current Field
	 * @throws InvalidExecOperationException
	 */
	private BasicDBObject getDbObject(TypeField typeField, long rank, long lstart, AtomicLong cpt) throws InvalidExecOperationException {
		BasicDBObject subobj = new BasicDBObject();
		String val = null;
		switch (typeField.type) {
			case constant:
				subobj.put(typeField.name, typeField.prefix);
				break;
			case constantArray:
				subobj.put(typeField.name, typeField.listeValeurs);
				break;
			case chaine:
				val = randomString(32);
				val = getValue(typeField, val);
				subobj.put(typeField.name, val);
				break;
			case date:
				long curdate = System.currentTimeMillis();
				Date date = new Date(curdate - rnd.nextInt());
				if (typeField.saveName != null) {
					context.savedNames.put(typeField.saveName, date.toString());
				}
				subobj.put(typeField.name, date);
				break;
			case nombre:
				Long lnval = rnd.nextLong();
				if (typeField.saveName != null) {
					context.savedNames.put(typeField.saveName, lnval.toString());
				}
				subobj.put(typeField.name, lnval);
				break;
			case save:
				val = getValue(typeField, "");
				subobj.put(typeField.name, val);
				break;
			case liste:
				int rlist = rnd.nextInt(typeField.listeValeurs.length);
				val = typeField.listeValeurs[rlist];
				val = getValue(typeField, val);
				subobj.put(typeField.name, val);
				break;
			case listeorder:
				long i = rank - lstart;
				if (i >= typeField.listeValeurs.length) {
					i = typeField.listeValeurs.length-1;
				}
				val = typeField.listeValeurs[(int) i];
				val = getValue(typeField, val);
				subobj.put(typeField.name, val);
				break;
			case serie:
				AtomicLong newcpt = cpt;
				long j = newcpt.get();
				if (typeField.idcpt != null) {
					newcpt = context.cpts.get(typeField.idcpt);
					//System.out.println("CPT replaced by: "+typeField.idcpt);
					if (newcpt == null) {
						newcpt = cpt;
						System.err.println("wrong cpt name: "+typeField.idcpt);
					} else {
						j = newcpt.getAndIncrement();
						//System.out.println("increment: "+j+" for "+typeField.name);
					}
				}
				if (typeField.modulo > 0) {
					j = j % typeField.modulo;
				}
				val = (typeField.prefix != null ? typeField.prefix : "")+j;
				val = getValue(typeField, val);
				subobj.put(typeField.name, val);
				break;
			case subfield:
				BasicDBObject subobjs = new BasicDBObject();
				for (TypeField field : typeField.subfields) {
					BasicDBObject obj = getDbObject(field, rank, lstart, cpt);
					if (obj != null) {
						subobjs.putAll((BSONObject) obj);
					} else {
						return null;
					}
				}
				subobj.put(typeField.name, (BSONObject) subobjs);
				break;
			case interval:
				int newval = rnd.nextInt(typeField.low, typeField.high+1);
				val = getValue(typeField, ""+newval);
				subobj.put(typeField.name, val);
				break;
			case select:
				BasicDBObject request = new BasicDBObject();
				for (TypeField field : typeField.subfields) {
					String name = field.name;
					String arg = getValue(field, "");
					request.append(name, arg);
				}
				if (simulate) {
					System.err.println("NotAsking: "+request);
					return null;
				}
				DAip maip = (DAip) MongoDbAccess.VitamCollections.Cdaip.getCollection().findOne(request, 
						new BasicDBObject(typeField.name, 1));
				if (maip != null) {
					//System.out.println("Select: "+typeField.name+":"+ maip.get(typeField.name));
					val = getValue(typeField, (String) maip.get(typeField.name));
					subobj.put(typeField.name, val);
				} else {
					//System.err.println("NotFound: "+request);
					return null;
				}
				break;
			default:
				throw new InvalidExecOperationException("Incorrect type: "+typeField.type);
		}
		return subobj;
	}
	
	private static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

	private final ThreadLocalRandom rnd = ThreadLocalRandom.current();

	private final String randomString(int len) {
	   StringBuilder sb = new StringBuilder( len );
	   for( int i = 0; i < len; i++ ) { 
	      sb.append(AB.charAt(rnd.nextInt(AB.length())));
	   }
	   return sb.toString();
	}

	private static final String example = "{ Domain: 'domainName', __model : 'modelName'," +
			" DAip : [ { __occur : 100},	{ champ1 : 'chaine', champ2 : { __type : 'liste', __liste : [ 'val1', 'val2' ] } }," +
			" { DAip : [ { __occur : 1000, __idcpt : 'moncpt' }, { champ3 : { __type : 'serie', __serie : { __prefix : 'Pref_', __idcpt : 'moncpt', __modulo: 100 } } }," +
			" { DAip : [ { __occur : 10 }, { champ4 : { __type : 'listeorder', __listeorder : [ 'val3', 'val4' ] }, " +
			" champ5 : { __type : 'serie', __serie : { __prefix : 'do_' } }," +
			" champ6 : { __type : 'subfield', __subfield : { subchamp : { __type : 'chaine' } } } }," +
					" { PAip : { champ7 : 'chaine' } } ] } ] } ] }";

	public static void main(String []args) {
		String ingest = example;
		if (args.length < 1) {
			System.err.println("Need a file ingest as argument => will use default test example");
		} else {
			try {
				ingest = FileUtil.readFile(args[0]);
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
		}
		try {
			ParserIngest ingested = new ParserIngest(true);
			ingested.parse(ingest);
			System.out.println(ingested.ingest);
			System.out.println(ingested.toString());
			System.out.println("Simulate Execution\n=====================");
			ingested.executeSimulate(11, 20);
		} catch (InvalidParseOperationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidExecOperationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
