/**
   This file is part of POC MongoDB ElasticSearch Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either versionRank 3 of the License, or
   (at your option) any later versionRank.

   POC MongoDB ElasticSearch is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with POC MongoDB ElasticSearch .  If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.mdbes;

import static fr.gouv.vitam.mdbes.MongoDbAccess.VitamCollections.Cdaip;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import fr.gouv.vitam.mdbes.MongoDbAccess.VitamLinks;
import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.UUID;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;


/**
 * @author "Frederic Bregier"
 *
 */
public class DAip extends VitamType {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(DAip.class);
    
    private static final long serialVersionUID = -2179544540441187504L;

    /**
     * DAIPDEPTHS : { UUID1 : depth2, UUID2 : depth2 }
     */
    public static final String DAIPDEPTHS = "_dds";
    /**
     * DAIPPARENTS : [ UUID1, UUID2 ]
     */
    public static final String DAIPPARENTS = "_dps";
    /**
     * Number of Immediate child (DAip)
     */
    public static final String NBCHILD = "_nb";
    
    /**
     * Number of Immediate child (DAip)
     */
    public long nb = 0;

    public DAip() {
        // empty 
    }

    @Override
    protected boolean updated(MongoDbAccess dbvitam) {
        DAip vt = (DAip) dbvitam.daips.collection.findOne(new BasicDBObject(ID, get(ID)));
        BasicDBObject update = null;
        if (vt != null) {
            LOGGER.debug("UpdateLinks: {}\n\t{}", this, vt);
            List<DBObject> list = new ArrayList<>();
            List<DBObject> listset = new ArrayList<>();
            /*
            Only parent link, not child link
            BasicDBObject upd = dbvitam.updateLinks(this, vt, VitamLinks.DAip2DAip, true);
            if (upd != null) { 
                list.add(upd);
            }
            */
            BasicDBObject upd = dbvitam.updateLinks(this, vt, VitamLinks.DAip2DAip, false);
            if (upd != null) { 
                list.add(upd);
            }
            upd = dbvitam.updateLinks(this, vt, VitamLinks.Domain2DAip, false);
            if (upd != null) { 
                list.add(upd);
            }
            upd = dbvitam.updateLink(this, vt, VitamLinks.DAip2PAip, true);
            if (upd != null) { 
                listset.add(upd);
            }
            upd = dbvitam.updateLink(this, vt, VitamLinks.DAip2Dua, true);
            if (upd != null) { 
                listset.add(upd);
            }
            // DAIPDEPTHS
            @SuppressWarnings("unchecked")
            HashMap<String, Integer> vtDom = (HashMap<String, Integer>) vt.removeField(DAIPDEPTHS);
            @SuppressWarnings("unchecked")
            HashMap<String, Integer> domainelevels = (HashMap<String, Integer>) get(DAIPDEPTHS);
            if (domainelevels == null) {
                domainelevels = new HashMap<String, Integer>();
            }
            BasicDBObject vtDomaineLevels = new BasicDBObject();
            if (vtDom != null) {
                // remove all not in current but in vt as already updated, for the others compare vt with current
                for (String dom : vtDom.keySet()) {
                    Integer pastval = (Integer) vtDom.get(dom);
                    Integer newval = (Integer) domainelevels.get(dom);
                    if (newval != null) {
                        if (pastval > newval) {
                            vtDomaineLevels.append(dom, newval); // to be remotely updated
                        } else {
                            vtDomaineLevels.append(dom, pastval); // to be remotely updated
                            domainelevels.put(dom, pastval); // update only locally
                        }
                    } else {
                        vtDomaineLevels.append(dom, pastval); // to be remotely updated
                        domainelevels.put(dom, pastval); // update only locally
                    }
                }
                // now add into remote update from current, but only non existing in vt (already done)
                for (String dom : domainelevels.keySet()) {
                    // remove by default
                    Integer srcobj = (Integer) vtDom.get(dom);
                    Integer obj = (Integer) domainelevels.get(dom);
                    if (srcobj == null) {
                        vtDomaineLevels.append(dom, obj); // will be updated remotely
                    }
                }
                // Update locally
                append(DAIPDEPTHS, domainelevels);
            }
            if (! vtDomaineLevels.isEmpty()) {
                upd = new BasicDBObject(DAIPDEPTHS, vtDomaineLevels);
                listset.add(upd);
            }
            try {
                update = new BasicDBObject();
                if (!list.isEmpty()) {
                    upd = new BasicDBObject();
                    for (DBObject dbObject : list) {
                        upd.putAll(dbObject);
                    }
                    update = update.append("$addToSet", upd);
                }
                if (!listset.isEmpty()) {
                    upd = new BasicDBObject();
                    for (DBObject dbObject : listset) {
                        upd.putAll(dbObject);
                    }
                    update = update.append("$set", upd);
                }
                update = update.append("$inc", new BasicDBObject(NBCHILD, nb));
                nb = 0;
                dbvitam.daips.collection.update(new BasicDBObject(ID, this.get(ID)), update);
            } catch (MongoException e) {
            	LOGGER.error("Exception for " + update, e);
                throw e;
            }
            list.clear();
            listset.clear();
            return true;
        } else {
            //dbvitam.updateLinks(this, null, VitamLinks.DAip2DAip, true);
            dbvitam.updateLinks(this, null, VitamLinks.DAip2DAip, false);
            dbvitam.updateLinks(this, null, VitamLinks.Domain2DAip, false);
            this.append(NBCHILD, nb);
            nb = 0;
        }
        return false;
    }

    public void saveToFile(MongoDbAccess dbvitam, int level) {
        if (level < GlobalDatas.minleveltofile) {
            save(dbvitam);
            return;
        }
        putBeforeSave();
        dbvitam.updateLinksToFile(this, VitamLinks.DAip2DAip, false);
        dbvitam.updateLinksToFile(this, VitamLinks.Domain2DAip, false);
        if (this.containsField(NBCHILD)) {
            long temp = this.getLong(NBCHILD);
            if (temp > 0) {
                nb += temp;
            }
        }
        this.append(NBCHILD, nb);
        /*String toprint = this.toStringDirect()+"\n";
        // XXX FIXME
        try {
            MainIngestFile.bufferedOutputStream.write(toprint.getBytes());
        } catch (IOException e) {
            LOGGER.error("Cannot save to File", e);
        }
        toprint = null;*/
        LOGGER.debug("{}", this);
    }

    public void save(MongoDbAccess dbvitam) {
        putBeforeSave();
        if (updated(dbvitam)) {
        	LOGGER.debug("Updated: {}", this); 
            return;
        }
        LOGGER.debug("Save: {}", this);
        updateOrSave(dbvitam.daips);
    }
    
    /**
     * Used in ingest (get the next dds including itself with depth +1 for all)
     * @return the new domdepth for children
     */
    public Map<String, Integer> getSubDomDepth() {
        String id = (String) get(ID);
        // must compute depth from parent
        HashMap<String, Integer> newdepth = new HashMap<>();
        // addAll to temporary HashMap
        @SuppressWarnings("unchecked")
        HashMap<String, Integer> vtDomaineLevels = (HashMap<String, Integer>) get(DAIPDEPTHS);
        if (vtDomaineLevels != null) {
            for (java.util.Map.Entry<String, Integer> entry : vtDomaineLevels.entrySet()) {
                newdepth.put(entry.getKey(), entry.getValue()+1);
            }
        }
        newdepth.put(id, 1);
        return newdepth;
    }
    /**
     * 
     * @return the map of dds with depth
     */
    @SuppressWarnings("unchecked")
    public Map<String, Integer> getDomDepth() {
        return (HashMap<String, Integer>) get(DAIPDEPTHS);
    }
    /**
     * Add the link N-N between DAip and List of sub DAip
     * @param dbvitam
     * @param daips
     */
    public void addDAip(MongoDbAccess dbvitam, List<DAip> daips) {
        DBObject update = null;
        List<String> ids = new ArrayList<>();
        for (DAip daip : daips) {
            DBObject update2 = dbvitam.addLink(this, VitamLinks.DAip2DAip, daip);
            if (update2 != null) {
                ids.add((String) daip.get(ID));
                update = update2;
            }
        }
        if (! ids.isEmpty()) {
            try {
                dbvitam.daips.collection.update(new BasicDBObject(ID, new BasicDBObject("$in", ids)), update, false, true);
            } catch (MongoException e) {
            	LOGGER.error("Exception for " + update, e);
                throw e;
            }
            nb += ids.size();
        }
        ids.clear();
    }
    
    /**
     * Add the link N-N between current father DAip and one child DAip
     * @param dbvitam
     * @param maips
     */
    public void addDAipWithNoSave(MongoDbAccess dbvitam, DAip maipChild) {
        if (MongoDbAccess.addAsymmetricLinksetNoSave(dbvitam.db, maipChild, VitamLinks.DAip2DAip.field2to1, this, true)) {
            nb += 1;
        }
    }

    /**
     * 
     * @param dbvitam
     * @return the list of UUID of children (database access)
     */
    public List<String> getChildrenDAipDBRefFromParent(MongoDbAccess dbvitam) {
        DBCursor cid = dbvitam.daips.collection.find(
                new BasicDBObject(MongoDbAccess.VitamLinks.DAip2DAip.field2to1, this.get(ID)), new BasicDBObject(ID, 1));
        List<String> ids = new ArrayList<>();
        while (cid.hasNext()) {
            String mid = (String) cid.next().get(ID);
            ids.add(mid);
        }
        cid.close();
        return ids;
    }
    /**
     * 
     * @param remove
     * @return the list of UUID of DAIP parents (immediate)
     */
    @SuppressWarnings("unchecked")
    public List<String> getFathersDAipDBRef(boolean remove) {
        if (remove) {
            return (List<String>) this.removeField(VitamLinks.DAip2DAip.field2to1);
        } else {
            return (List<String>) this.get(VitamLinks.DAip2DAip.field2to1);
        }
    }
    /**
     * 
     * @param remove
     * @return the list of UUID of DOMAIN parents (immediate)
     */
    @SuppressWarnings("unchecked")
    public List<String> getFathersDomaineDBRef(boolean remove) {
        if (remove) {
            return (List<String>) this.removeField(VitamLinks.Domain2DAip.field2to1);
        } else {
            return (List<String>) this.get(VitamLinks.Domain2DAip.field2to1);
        }
    }

    /**
     * Add the link 1- between DAip and DuaRef
     * @param dbvitam
     * @param dua
     */
    public void addDuaRef(MongoDbAccess dbvitam, DuaRef dua) {
        dbvitam.addLink(this, VitamLinks.DAip2Dua, dua);
    }
    public String getDuaRefDBRef(boolean remove) {
        if (remove) {
            return (String) this.removeField(VitamLinks.DAip2Dua.field1to2);
        } else {
            return (String) this.get(VitamLinks.DAip2Dua.field1to2);
        }
    }
    /**
     * Add the link 1-N between DAip and PAip
     * @param dbvitam
     * @param data
     */
    public void addPAip(MongoDbAccess dbvitam, PAip data) {
        DBObject update = dbvitam.addLink(this, VitamLinks.DAip2PAip, data);
        if (update != null) {
            data.update(dbvitam.paips, update);
        }
    }
    /**
     * 
     * @param remove
     * @return the PAIP UUID
     */
    public String getPAipDBRef(boolean remove) {
        if (remove) {
            return (String) this.removeField(VitamLinks.DAip2PAip.field1to2);
        } else {
            return (String) this.get(VitamLinks.DAip2PAip.field1to2);
        }
    }
    
    /**
     * Check if the current DAip has path as immediate parent (either being a DAip or a Domain)
     * @param path
     * @return True if immediate parent, else False (however could be a grand parent)
     */
    public boolean isImmediateParent(String path) {
        String lastp = UUID.getLastAsString(path);
        List<String> immediateParents = getFathersDomaineDBRef(false);
        if (immediateParents != null && ! immediateParents.isEmpty()) {
            if (immediateParents.contains(lastp)) {
                return true;
            }
        }
        immediateParents = getFathersDAipDBRef(false);
        if (immediateParents != null && ! immediateParents.isEmpty()) {
            if (immediateParents.contains(lastp)) {
                return true;
            }
        }
        return false;
    }
    /**
     * 
     * @param dbvitam
     * @param path
     * @return the list of valid pathes (protentiialy empty) from this DAip to path, final path not containing neither path, neither this DAip
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public List<String> getPathesToParent(MongoDbAccess dbvitam, String path) throws InstantiationException, IllegalAccessException {
        List<String> pathes = new ArrayList<>();
        if (isImmediateParent(path)) {
            pathes.add("");
            return pathes;
        }
        String lastp = UUID.getLastAsString(path);
        List<String> result = new ArrayList<>();
        List<String> subpath = new ArrayList<>();
        subpath.add("");
        getSubPathesToParent(dbvitam, lastp, this, result, subpath);
        subpath.clear();
        subpath = null;
        return result;
    }
    /**
     * Compute all possible path from current to target (being Domaine or DAip)
     * @param dbvitam
     * @param target
     * @param current
     * @param pathResults final list containing all valid pathes (excluding target and current as implicit first and very last)
     * @param subpathesCurrent list of intermediary pathes from very first current (not included)
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private static void getSubPathesToParent(MongoDbAccess dbvitam, String target, DAip current,
            List<String> pathResults, List<String> subpathesCurrent)
            throws InstantiationException, IllegalAccessException {
        List<String> immediateParents = current.getFathersDAipDBRef(false);
        List<String> newSubpathes = new ArrayList<>();
        for (String immediateParent : immediateParents) {
            DAip parent = DAip.findOne(dbvitam, immediateParent);
            if (parent == null) {
                continue;
            }
            if (parent.isImmediateParent(target)) {
                // End of search for this parent: target/parent/current are directly attached
                for (String subpath : subpathesCurrent) {
                    pathResults.add(immediateParent+subpath);
                }
            } else if (parent.getDomDepth().containsKey(target)) {
                // Still in path: current becomes immediateParent and subpathesCurrent to new Subpathes
                for (String subpath : subpathesCurrent) {
                    newSubpathes.add(immediateParent+subpath);
                }
                getSubPathesToParent(dbvitam, target, parent, pathResults, newSubpathes);
                newSubpathes.clear();
            }
            // Else not in the path so ignore this parent
        }
        newSubpathes = null;
    }
    
    public final void cleanStructure(boolean all) {
        removeField(VitamLinks.DAip2DAip.field1to2);
        removeField(VitamLinks.DAip2DAip.field2to1);
        removeField(VitamLinks.Domain2DAip.field2to1);
        removeField(VitamLinks.DAip2Dua.field1to2);
        removeField(VitamLinks.DAip2PAip.field1to2);
        removeField("_nb");
        removeField(ID);
        removeField("_refid");
        if (all) {
            removeField(DAIPDEPTHS);
            removeField(NBCHILD);
        }
    }
    
    /**
     * Should be called only once saved (last time), but for the moment let the object as it is, next should remove not indexable entries
     * @param dbvitam
     */
    public void addEsIndex(MongoDbAccess dbvitam, Map<String, String> indexes, String model) {
        BasicDBObject maip = (BasicDBObject) this.copy();
        if (! maip.containsField(NBCHILD)) {
            maip.append(NBCHILD, this.nb);
        }
        maip.removeField(VitamLinks.DAip2DAip.field1to2);
        maip.removeField(VitamLinks.DAip2DAip.field2to1);
        maip.removeField(VitamLinks.Domain2DAip.field2to1);
        maip.removeField(VitamLinks.DAip2Dua.field1to2);
        maip.removeField(VitamLinks.DAip2PAip.field1to2);
        maip.removeField(ID);
        // DOMDEPTH already ok but duplicate it
        @SuppressWarnings("unchecked")
        HashMap<String, Integer> map = (HashMap<String, Integer>) maip.get(DAIPDEPTHS);
        List<String> list = new ArrayList<>();
        list.addAll(map.keySet());
        maip.append(DAIPPARENTS, list);
        LOGGER.debug("{}", this);
        indexes.put((String) this.get(ID), maip.toString());
        if (indexes.size() > GlobalDatas.LIMIT_ES_NEW_INDEX) {
            if (GlobalDatas.BLOCKING) {
                dbvitam.es.addEntryIndexesBlocking(GlobalDatas.INDEXNAME, model, indexes);
            } else {
                dbvitam.es.addEntryIndexes(GlobalDatas.INDEXNAME, model, indexes);
            }
            
            //dbvitam.flushOnDisk();
            indexes.clear();
        }
        maip.clear();
        maip = null;
    }
    
    @Override
    public void load(MongoDbAccess dbvitam) {
        DAip vt = (DAip) dbvitam.daips.collection.findOne(new BasicDBObject(ID, get(ID)));
        this.putAll((BSONObject) vt);
    }

    public static DAip findOne(MongoDbAccess dbvitam, String refid) throws InstantiationException, IllegalAccessException {
        return (DAip) dbvitam.findOne(Cdaip, refid);
    }
    
    public static void addIndexes(MongoDbAccess dbvitam) {
        dbvitam.daips.collection.createIndex(new BasicDBObject(MongoDbAccess.VitamLinks.Domain2DAip.field2to1, 1));
        // if not set, MAIP and Tree are worst
        //dbvitam.metaaips.collection.createIndex(new BasicDBObject(MongoDbAccess.VitamLinks.DAip2DAip.field1to2, 1));
        dbvitam.daips.collection.createIndex(new BasicDBObject(MongoDbAccess.VitamLinks.DAip2DAip.field2to1, 1));
        dbvitam.daips.collection.createIndex(new BasicDBObject(MongoDbAccess.VitamLinks.DAip2PAip.field1to2, 1));

        // does not improve anything 
        dbvitam.daips.collection.createIndex(new BasicDBObject(DAIPDEPTHS, 1));
        // Depth requests are Worst if set 
        //dbvitam.metaaips.collection.createIndex(indexDomDepth);
        //dbvitam.metaaips.collection.createIndex(indexDom);
        
        // Business
        //dbvitam.metaaips.collection.createIndex(new BasicDBObject("name", 1));
    }

}
