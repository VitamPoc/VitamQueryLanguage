/**
   This file is part of POC MongoDB ElasticSearch Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   POC MongoDB ElasticSearch is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with POC MongoDB ElasticSearch .  If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.query.old.exec;

import static fr.gouv.vitam.mdbtypes.MongoDbAccess.VitamCollections.Cdaip;
import static fr.gouv.vitam.mdbtypes.MongoDbAccess.VitamCollections.Cdua;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBObject;

import fr.gouv.vitam.mdbtypes.DAip;
import fr.gouv.vitam.mdbtypes.Domain;
import fr.gouv.vitam.mdbtypes.DuaRef;
import fr.gouv.vitam.mdbtypes.MongoDbAccess;
import fr.gouv.vitam.query.exception.InvalidExecOperationException;

public class ResultRequest {
    public int minDepth = 0;
    public int maxDepth = 0;
    public Set<String> lastIds;
    public List<Map<String, Integer>> allPaths;
    public long nextIds;
    public List<BasicDBObject> finalResult;
    
    public void clearStruct() {
        if (this.lastIds != null) {
            this.lastIds.clear();
            this.lastIds = null;
        }
        if (this.allPaths != null) {
            for (Map<String, Integer> hash : allPaths) {
                hash.clear();
            }
            allPaths.clear();
            allPaths = null;
        }
    }

    public void clear() {
        this.minDepth = 0;
        this.maxDepth = 0;
        nextIds = 0;
        clearStruct();
    }
    
    public void init() {
        clear();
        this.lastIds = new HashSet<>();
        this.allPaths = new ArrayList<>();
    }
    
    public int size() {
        return (this.lastIds == null ? 0 : this.lastIds.size());
    }
    
    public boolean isEmpty() {
        return (this.lastIds == null ? true : this.lastIds.isEmpty());
    }
    
    public static Map<String, Integer> sortByValue(Map<String, Integer> map) {
         List<Map.Entry<String, Integer>>  list = new ArrayList<>(map.entrySet());
         Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
              public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                   return o2.getValue().compareTo(o1.getValue()); // inverse order
              }
         });
        Map<String, Integer> result = new LinkedHashMap<>();
        for (Iterator<Map.Entry<String, Integer>> it = list.iterator(); it.hasNext();) {
            Map.Entry<String, Integer> entry = it.next();
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
    
    /**
     * 
     * @param dbvitam
     * @param maip
     * @return the flatten BSONObject for this MAIP
     */
    private static final BasicDBObject getMaipResult(MongoDbAccess dbvitam, DAip maip) {
        BasicDBObject newfinal = null;
        DuaRef duar = null;
        String id = maip.getDuaRefDBRef(true);
        if (id != null) {
            duar = (DuaRef) dbvitam.loadFromObjectId(dbvitam.duarefs, id);
            duar.removeField("_id");
            id = null;
        }
        maip.cleanStructure(true);
        newfinal = new BasicDBObject(maip);
        if (duar != null) {
            newfinal.append(Cdua.name, duar);
        }
        return newfinal;
    }

    /**
     * 
     * @param dbvitam
     * @param oid
     * @param level
     * @param prevAip
     * @return the flatten BSONObject for one Maip/Domain and its subelements
     */
    private static final BasicDBObject addPrevLevel(MongoDbAccess dbvitam, String oid, Integer level, BasicDBObject prevAip) {
        BasicDBObject newfinal = null;
        if (level > 0) {
            DAip maip = (DAip) dbvitam.loadFromObjectId(dbvitam.daips, oid);
            if (maip != null) {
                // should be
                newfinal = getMaipResult(dbvitam, maip);
                if (prevAip != null) {
                    newfinal = newfinal.append(Cdaip.name, prevAip);
                }
            }
        } else {
            // Domaine
            Domain domaine = (Domain) dbvitam.loadFromObjectId(dbvitam.domains, oid);
            if (domaine != null) {
                // should be
                domaine.cleanStructure();
                newfinal = new BasicDBObject(domaine);
                if (prevAip != null) {
                    newfinal = newfinal.append(Cdaip.name, prevAip);
                }
            }
        }
        return newfinal;
    }
    
    /**
     * 
     * @param dbvitam
     * @param idson
     * @param levson
     * @param father
     * @param levfather
     * @param prevMaip
     * @return the flatten BSONObject that hold all Maip from idson up to father (exclude)
     * @throws InvalidExecOperationException 
     */
    private static final BasicDBObject getFullPath(MongoDbAccess dbvitam, 
            String idson, Integer levson, 
            String father, Integer levfather, BasicDBObject prevMaip, boolean first) throws InvalidExecOperationException {
        BasicDBObject newfinal = null;
        if (levson > levfather+1) {
            // need to go by multiple steps
            //System.out.println("Go up depth: "+ levson+":"+levfather);
            DAip maip = (DAip) dbvitam.loadFromObjectId(dbvitam.daips, idson);
            List<String> fathers = null;
            if (maip != null) {
                fathers = maip.getFathersDAipDBRef(false);
            }
            if (fathers == null) {
                throw new InvalidExecOperationException("Go up BUT no fathers! "+maip.toString());
            }
            BasicDBObject intermediate = null;
            if (first) {
                intermediate = prevMaip;
            } else {
                intermediate = getMaipResult(dbvitam, maip);
                if (prevMaip != null) {
                    intermediate.append(Cdaip.name, prevMaip);
                }
            }
            if (maip != null) {
                //System.out.println(" Fathers: "+(fathers == null ? -1 : fathers.size()));
                for (String objectId : fathers) {
                    BasicDBObject curFather = getFullPath(dbvitam, objectId, levson-1, father, levfather, intermediate, false);
                    if (curFather != null) {
                        // to keep (only one is valid)
                        newfinal = curFather;
                        //System.out.println("found: "+objectId);
                        break;
                    } else {
                        //System.out.println("not found multiple: "+objectId);
                    }
                }
                //System.out.println("found? "+ (newfinal != null));
            } else {
                throw new InvalidExecOperationException("No MAIP while getFullPath: " + levson+":"+levfather);
            }
        } else {
            // final level so check now that we found the correct father
            //System.out.println("search father of : "+idson+" =? "+father);
            DAip maip = (DAip) dbvitam.loadFromObjectId(dbvitam.daips, idson);
            //System.out.println(maip);
            List<String> fathers = null;
            if (levson == 1) {
                fathers = maip.getFathersDomaineDBRef(true);
            } else {
                fathers = maip.getFathersDAipDBRef(true);
            }
            if (fathers.contains(father)) {
                // from children only, since father will be added after
                newfinal = getMaipResult(dbvitam, maip);
                if (prevMaip != null) {
                    newfinal = newfinal.append(Cdaip.name, prevMaip);
                }
                //System.out.println("ok path simple");
                return newfinal;
            } else {
                /*System.out.println("wrong path simple");
                System.out.println("Father: "+father+":"+(fathers != null ? fathers.size() : -1));
                for (String objectId : fathers) {
                    System.out.println("\tFathers: "+objectId);
                }*/
                // ignore, wrong path
                return null;
            }
        }
        return newfinal;
    }

    /**
     * 
     * @param dbvitam
     * @return the final MetaAip result list (flattened)
     * @throws InvalidExecOperationException 
     */
    public final List<BasicDBObject> finalizeFullPath(MongoDbAccess dbvitam) throws InvalidExecOperationException {
        // For each paths, check the full path and build final full MetaAips
        List<BasicDBObject> finalMaips = new ArrayList<>();
        List<Map<String, Integer>> paths = allPaths;
        for (Map<String, Integer> hashMap : paths) {
            BasicDBObject finalMaip = null;
            String prevOid = null;
            Integer prevLevel = null;
            for (String oid : hashMap.keySet()) {
                if (prevOid == null) {
                    // first
                    prevOid = oid;
                    prevLevel = hashMap.get(oid);
                    finalMaip = addPrevLevel(dbvitam, oid, prevLevel, null);
                } else {
                    // next one
                    Integer level = hashMap.get(oid);
                    if (level+1 < prevLevel) {
                        // need to find the full path
                        finalMaip = getFullPath(dbvitam, prevOid, prevLevel, oid, level, finalMaip, true);
                        if (finalMaip == null) {
                            break;
                        }
                    }
                    finalMaip = addPrevLevel(dbvitam, oid, level, finalMaip);
                    prevOid = oid;
                    prevLevel = level;
                }
            }
            if (finalMaip != null) {
                finalMaips.add(finalMaip);
            }
        }
        //System.out.println("FinalMaips: "+finalMaips.size());
        return finalMaips;
    }

}