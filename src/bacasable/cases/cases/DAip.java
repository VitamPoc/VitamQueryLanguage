/**
 * This file is part of POC MongoDB ElasticSearch Project.
 *
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published
 * by the Free Software Foundation, either versionRank 3 of the License, or
 * (at your option) any later versionRank.
 *
 * POC MongoDB ElasticSearch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with POC MongoDB ElasticSearch . If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.cases;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

import fr.gouv.vitam.cases.CassandraAccess.VitamCollections;
import fr.gouv.vitam.cases.CassandraAccess.VitamLinks;
import fr.gouv.vitam.utils.UUID;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * DAip object (Data)
 *
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
     *
     */
    public DAip() {
        // empty
    }

    /**
     * Update the object to the database
     *
     * @param update
     */
    public void update(final DBObject update) {
        Cdaip.getCollection().update(new BasicDBObject(ID, getId()), update);
    }

    @Override
    protected boolean updated(final CassandraAccess dbvitam) {
        final DAip vt = (DAip) dbvitam.daips.getObject(id);
        if (vt != null) {
            LOGGER.debug("UpdateLinks: {}\n\t{}", this, vt);
            
            /*
             * Only parent link, not child link
             * BasicDBObject upd = dbvitam.updateLinks(this, vt, VitamLinks.DAip2DAip, true);
             * if (upd != null) {
             * list.add(upd);
             * }
             */
            Set<String> updUp = this.updateUpLinks(vt);
            Set<String> updDom = this.updateDomsLinks(vt);
            String updPaip = this.updatePaip(vt);
            Set<String> updDua = this.updateDuasLinks(vt);

            // DAIPDEPTHS
            final Map<String, Integer> vtDom = vt.dps;
            Map<String, Integer> domainelevels = dps;
            if (domainelevels == null) {
                domainelevels = new HashMap<String, Integer>();
                dps = domainelevels;
            }
            final Map<String, Integer> vtDomaineLevels = new HashMap<String, Integer>();
            if (vtDom != null) {
                // remove all not in current but in vt as already updated, for the others compare vt with current
                for (final String dom : vtDom.keySet()) {
                    final Integer pastval = vtDom.get(dom);
                    final Integer newval = domainelevels.get(dom);
                    if (newval != null) {
                        if (pastval > newval) {
                            vtDomaineLevels.put(dom, newval); // to be remotely updated
                        } else {
                            vtDomaineLevels.put(dom, pastval); // to be remotely updated
                            domainelevels.put(dom, pastval); // update only locally
                        }
                    } else {
                        vtDomaineLevels.put(dom, pastval); // to be remotely updated
                        domainelevels.put(dom, pastval); // update only locally
                    }
                }
                // now add into remote update from current, but only non existing in vt (already done)
                for (final String dom : domainelevels.keySet()) {
                    // remove by default
                    final Integer srcobj = vtDom.get(dom);
                    final Integer obj = domainelevels.get(dom);
                    if (srcobj == null) {
                        vtDomaineLevels.put(dom, obj); // will be updated remotely
                    }
                }
            }
            StringBuilder builder = new StringBuilder("UPDATE "+VitamCollections.Cdaip.getName()+" SET ");
            try {
                if (updUp != null) {
                    builder.append(VitamLinks.DAip2DAip.field2to1 + " = " + VitamLinks.DAip2DAip.field2to1 + " + ?, ");
                }
                if (updDom != null) {
                    builder.append(VitamLinks.Domain2DAip.field2to1 + " = " + VitamLinks.Domain2DAip.field2to1 + " + ?, ");
                }
                if (updDua != null) {
                    builder.append(VitamLinks.DAip2Dua.field1to2 + " = " + VitamLinks.DAip2Dua.field1to2 + " + ?, ");
                }
                if (updPaip != null) {
                    builder.append(VitamLinks.DAip2PAip.field1to2 + " = ?, ");
                }
                if (vtDomaineLevels != null) {
                    builder.append(DAIPDEPTHS + " = " + DAIPDEPTHS + " + ?, ");
                }
                builder.append(NBCHILD + " = " + NBCHILD + " + " + nb);
                nb = 0;
                builder.append(" WHERE " + VitamType.ID + " = ?;");
                PreparedStatement writeStatement = dbvitam.session.prepare(builder.toString());
                BoundStatement bs = writeStatement.bind();
                int i = 0;
                if (updUp != null) {
                    bs.setSet(i++, updUp);
                }
                if (updDom != null) {
                    bs.setSet(i++, updDom);
                }
                if (updDua != null) {
                    bs.setSet(i++, updDua);
                }
                if (updPaip != null) {
                    bs.setString(i++, updPaip);
                }
                if (vtDomaineLevels != null) {
                    bs.setMap(i++, vtDomaineLevels);
                }
                bs.setString(i, id);
                dbvitam.session.execute(bs);
            } catch (final Exception e) {
                LOGGER.error("Exception for " + builder.toString(), e);
                throw e;
            }
            return true;
        } else {
            // dbvitam.updateLinks(this, null, VitamLinks.DAip2DAip, true);
            this.updateUpLinks(null);
            this.updateDomsLinks(null);
        }
        return false;
    }

    /**
     * Special method to save to file and not to database
     *
     * @param dbvitam
     * @param outputStream
     */
    public void saveToFile(final CassandraAccess dbvitam, final OutputStream outputStream) {
        putBeforeSave();
        dbvitam.updateLinksToFile(this, VitamLinks.DAip2DAip, false);
        dbvitam.updateLinksToFile(this, VitamLinks.Domain2DAip, false);
        if (containsField(NBCHILD)) {
            final long temp = this.getLong(NBCHILD);
            if (temp > 0) {
                nb += temp;
            }
        }
        append(NBCHILD, nb);
        toFile(outputStream);
    }

    /**
     * Special method to save to file and not to database
     *
     * @param dbvitam
     * @param outputStream
     */
    public void toFile(final OutputStream outputStream) {
        String toprint = toStringDirect() + "\n";
        try {
            outputStream.write(toprint.getBytes());
        } catch (final IOException e) {
            LOGGER.error("Cannot save to File", e);
        }
        toprint = null;
    }

    @Override
    public void save(final CassandraAccess dbvitam) {
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
     *
     * @return the new domdepth for children
     */
    public Map<String, Integer> getSubDomDepth() {
        final String id = (String) get(ID);
        // must compute depth from parent
        final HashMap<String, Integer> newdepth = new HashMap<>();
        // addAll to temporary HashMap
        @SuppressWarnings("unchecked")
        final HashMap<String, Integer> vtDomaineLevels = (HashMap<String, Integer>) get(DAIPDEPTHS);
        if (vtDomaineLevels != null) {
            for (final java.util.Map.Entry<String, Integer> entry : vtDomaineLevels.entrySet()) {
                newdepth.put(entry.getKey(), entry.getValue() + 1);
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
        return (Map<String, Integer>) get(DAIPDEPTHS);
    }
    /**
     * 
     * @return the max depth of this node from existing parents
     */
    public int getMaxDepth() {
        Map<String, Integer> map = getDomDepth();
        int depth = 0;
        for (Iterator<Integer> iterator = map.values().iterator(); iterator.hasNext();) {
            Integer type = (Integer) iterator.next();
            if (depth < type) {
                depth = type;
            }
        }
        return depth+1;
    }
    /**
     * Add the link N-N between DAip and List of sub DAip
     *
     * @param dbvitam
     * @param daips
     */
    public void addDAip(final CassandraAccess dbvitam, final List<DAip> daips) {
        DBObject update = null;
        final List<String> ids = new ArrayList<>();
        for (final DAip daip : daips) {
            final DBObject update2 = dbvitam.addLink(this, VitamLinks.DAip2DAip, daip);
            if (update2 != null) {
                ids.add((String) daip.get(ID));
                update = update2;
            }
        }
        if (!ids.isEmpty()) {
            try {
                dbvitam.daips.collection.update(new BasicDBObject(ID, new BasicDBObject("$in", ids)), update, false, true);
            } catch (final MongoException e) {
                LOGGER.error("Exception for " + update, e);
                throw e;
            }
            nb += ids.size();
        }
        ids.clear();
    }

    /**
     * Add the link N-N between current father DAip and one child DAip
     *
     * @param maipChild
     */
    public void addDAipWithNoSave(final DAip maipChild) {
        if (CassandraAccess.addAsymmetricLinksetNoSave(maipChild, VitamLinks.DAip2DAip.field2to1, this)) {
            nb += 1;
        }
    }

    /**
     *
     * @param dbvitam
     * @return the list of UUID of children (database access)
     */
    public List<String> getChildrenDAipDBRefFromParent(final CassandraAccess dbvitam) {
        final DBCursor cid = dbvitam.daips.collection.find(
                new BasicDBObject(CassandraAccess.VitamLinks.DAip2DAip.field2to1, this.get(ID)), new BasicDBObject(ID, 1));
        final List<String> ids = new ArrayList<>();
        while (cid.hasNext()) {
            final String mid = (String) cid.next().get(ID);
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
    public List<String> getFathersDAipDBRef(final boolean remove) {
        if (remove) {
            return (List<String>) removeField(VitamLinks.DAip2DAip.field2to1);
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
    public List<String> getFathersDomaineDBRef(final boolean remove) {
        if (remove) {
            return (List<String>) removeField(VitamLinks.Domain2DAip.field2to1);
        } else {
            return (List<String>) this.get(VitamLinks.Domain2DAip.field2to1);
        }
    }

    /**
     * Add the link 1- between DAip and DuaRef
     *
     * @param dbvitam
     * @param dua
     */
    public void addDuaRef(final CassandraAccess dbvitam, final DuaRef dua) {
        dbvitam.addLink(this, VitamLinks.DAip2Dua, dua);
    }

    /**
     * @param remove
     * @return the associated DUA reference
     */
    public String getDuaRefDBRef(final boolean remove) {
        if (remove) {
            return (String) removeField(VitamLinks.DAip2Dua.field1to2);
        } else {
            return (String) this.get(VitamLinks.DAip2Dua.field1to2);
        }
    }

    /**
     * Add the link 1-N between DAip and PAip
     *
     * @param dbvitam
     * @param data
     */
    public void addPAip(final CassandraAccess dbvitam, final PAip data) {
        final DBObject update = dbvitam.addLink(this, VitamLinks.DAip2PAip, data);
        if (update != null) {
            data.update(dbvitam.paips, update);
        }
    }

    /**
     *
     * @param remove
     * @return the PAIP UUID
     */
    public String getPAipDBRef(final boolean remove) {
        if (remove) {
            return (String) removeField(VitamLinks.DAip2PAip.field1to2);
        } else {
            return (String) this.get(VitamLinks.DAip2PAip.field1to2);
        }
    }

    /**
     * Check if the current DAip has path as immediate parent (either being a DAip or a Domain)
     *
     * @param path
     * @return True if immediate parent, else False (however could be a grand parent)
     */
    public boolean isImmediateParent(final String path) {
        final String lastp = UUID.getLastAsString(path);
        List<String> immediateParents = getFathersDomaineDBRef(false);
        if (immediateParents != null && !immediateParents.isEmpty()) {
            if (immediateParents.contains(lastp)) {
                return true;
            }
        }
        immediateParents = getFathersDAipDBRef(false);
        if (immediateParents != null && !immediateParents.isEmpty()) {
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
     * @return the list of valid pathes (protentiialy empty) from this DAip to path, final path not containing neither path,
     *         neither this DAip
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public List<String> getPathesToParent(final CassandraAccess dbvitam, final String path) throws InstantiationException,
    IllegalAccessException {
        final List<String> pathes = new ArrayList<>();
        if (isImmediateParent(path)) {
            pathes.add("");
            return pathes;
        }
        final String lastp = UUID.getLastAsString(path);
        final List<String> result = new ArrayList<>();
        List<String> subpath = new ArrayList<>();
        subpath.add("");
        getSubPathesToParent(dbvitam, lastp, this, result, subpath);
        subpath.clear();
        subpath = null;
        return result;
    }

    /**
     * Compute all possible path from current to target (being Domaine or DAip)
     *
     * @param dbvitam
     * @param target
     * @param current
     * @param pathResults
     *            final list containing all valid pathes (excluding target and current as implicit first and very last)
     * @param subpathesCurrent
     *            list of intermediary pathes from very first current (not included)
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private static void getSubPathesToParent(final CassandraAccess dbvitam, final String target, final DAip current,
            final List<String> pathResults, final List<String> subpathesCurrent) throws InstantiationException,
            IllegalAccessException {
        final List<String> immediateParents = current.getFathersDAipDBRef(false);
        List<String> newSubpathes = new ArrayList<>();
        for (final String immediateParent : immediateParents) {
            final DAip parent = DAip.findOne(dbvitam, immediateParent);
            if (parent == null) {
                continue;
            }
            if (parent.isImmediateParent(target)) {
                // End of search for this parent: target/parent/current are directly attached
                for (final String subpath : subpathesCurrent) {
                    pathResults.add(immediateParent + subpath);
                }
            } else if (parent.getDomDepth().containsKey(target)) {
                // Still in path: current becomes immediateParent and subpathesCurrent to new Subpathes
                for (final String subpath : subpathesCurrent) {
                    newSubpathes.add(immediateParent + subpath);
                }
                getSubPathesToParent(dbvitam, target, parent, pathResults, newSubpathes);
                newSubpathes.clear();
            }
            // Else not in the path so ignore this parent
        }
        newSubpathes = null;
    }

    /**
     * Used in loop operation to clean the object
     *
     * @param all
     *            If true, all items are cleaned
     */
    public final void cleanStructure(final boolean all) {
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
     * Should be called only once saved (last time), but for the moment let the object as it is, next should remove not indexable
     * entries
     *
     * @param dbvitam
     * @param indexes
     * @param model
     * @return the number of DAip inserted in ES
     */
    public int addEsIndex(final CassandraAccess dbvitam, final Map<String, String> indexes, final String model) {
        if (!this.containsField(NBCHILD)) {
            this.append(NBCHILD, nb);
            final int nb = ElasticSearchAccess.addEsIndex(dbvitam, model, indexes, this);
            this.removeField(NBCHILD);
            return nb;
        } else {
            return ElasticSearchAccess.addEsIndex(dbvitam, model, indexes, this);
        }
    }
    /**
     * 
     * @param dbvitam
     * @param model
     * @return True if inserted in ES
     */
    public boolean addEsIndex(final CassandraAccess dbvitam, final String model) {
        BasicDBObject maip = (BasicDBObject) copy();
        if (!maip.containsField(NBCHILD)) {
            maip.append(NBCHILD, nb);
        }
        return ElasticSearchAccess.addEsIndex(dbvitam, model, maip);
    }
    
    @Override
    public boolean load(final CassandraAccess dbvitam) {
        final DAip vt = (DAip) dbvitam.daips.collection.findOne(getString(ID));
        if (vt == null) {
            return false;
        }
        this.putAll((BSONObject) vt);
        getAfterLoad();
        return true;
    }

    /**
     * @param dbvitam
     * @param refid
     * @return the DAip
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static DAip findOne(final CassandraAccess dbvitam, final String refid) throws InstantiationException,
            IllegalAccessException {
        return (DAip) dbvitam.findOne(Cdaip, refid);
    }

    protected static void addIndexes(final CassandraAccess dbvitam) {
        dbvitam.daips.createIndex(CassandraAccess.VitamLinks.Domain2DAip.field2to1);
        // if not set, MAIP and Tree are worst
        // dbvitam.metaaips.collection.createIndex(new BasicDBObject(MongoDbAccess.VitamLinks.DAip2DAip.field1to2, 1));
        dbvitam.daips.createIndex(CassandraAccess.VitamLinks.DAip2DAip.field2to1);
        dbvitam.daips.createIndex(CassandraAccess.VitamLinks.DAip2PAip.field1to2);
        dbvitam.daips.createIndex(CassandraAccess.VitamLinks.DAip2Dua.field1to2);

        // does not improve anything
        dbvitam.daips.createIndex(DAIPDEPTHS);
        // Depth requests are Worst if set
        // dbvitam.metaaips.collection.createIndex(indexDomDepth);
        // dbvitam.metaaips.collection.createIndex(indexDom);

        // Business
        // dbvitam.metaaips.collection.createIndex(new BasicDBObject("name", 1));
    }

    protected static String createTable() {
        return ", "+CassandraAccess.VitamLinks.Domain2DAip.field2to1+ " set<varchar>"+
                ", "+CassandraAccess.VitamLinks.DAip2DAip.field2to1+" set<varchar>"+
                ", "+CassandraAccess.VitamLinks.DAip2PAip.field1to2+ " set<varchar>"+
                ", "+CassandraAccess.VitamLinks.DAip2Dua.field1to2+ " set<varchar>"+
                ", "+DAIPDEPTHS+ " map<varchar, int>"+
                ", "+DAIPPARENTS+ " set<varchar>"+
                ", "+NBCHILD+ " bigint";
    }
}
