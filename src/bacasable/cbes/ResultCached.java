/**
 * This file is part of POC MongoDB ElasticSearch Project.
 *
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * POC MongoDB ElasticSearch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with POC MongoDB ElasticSearch . If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.cbes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import fr.gouv.vitam.cbes.CouchbaseAccess.VitamCollections;
import fr.gouv.vitam.utils.UUID;

/**
 * Result (potentially cached) object
 *
 * @author "Frederic Bregier"
 *
 */
public class ResultCached extends VitamType {
    /**
     * Current SAip in the result
     */
    public static final String CURRENTDAIP = "__cdaip";
    /**
     * Min depth
     */
    public static final String MINLEVEL = "__min";
    /**
     * Max depth
     */
    public static final String MAXLEVEL = "__max";
    /**
     * Number of sub nodes
     */
    public static final String NBSUBNODES = "__nbnd";

    /**
     * Current SAip in the result
     */
    public Set<String> currentDaip = new HashSet<String>();
    /**
     * Min depth
     */
    public int minLevel = 0;
    /**
     * Max depth
     */
    public int maxLevel = 0;
    /**
     * Number of sub nodes
     */
    public long nbSubNodes = -1;
    /**
     * If loaded (or saved) to the database = True
     */
    public boolean loaded = false;

    /**
     *
     */
    public ResultCached() {

    }

    /**
     * @param collection
     */
    public ResultCached(final Collection<String> collection) {
        currentDaip.addAll(collection);
        updateMinMax();
        // Path list so as loaded (never cached)
        loaded = true;
        putBeforeSave();
    }

    @Override
    public void clear() {
        super.clear();
        currentDaip.clear();
        minLevel = 0;
        maxLevel = 0;
        nbSubNodes = -1;
        loaded = false;
    }

    @Override
    public void getAfterLoad() {
        super.getAfterLoad();
        if (containsField(CURRENTDAIP)) {
            @SuppressWarnings("unchecked")
            final Set<String> list = (Set<String>) this.get(CURRENTDAIP);
            currentDaip.clear();
            currentDaip.addAll(list);
        }
        if (this.containsField(MINLEVEL)) {
            minLevel = this.getInt(MINLEVEL);
        } else {
            minLevel = 0;
        }
        if (this.containsField(MAXLEVEL)) {
            maxLevel = this.getInt(MAXLEVEL);
        } else {
            maxLevel = 0;
        }
        if (this.containsField(NBSUBNODES)) {
            nbSubNodes = this.getLong(NBSUBNODES);
        } else {
            nbSubNodes = -1;
        }
    }

    @Override
    public void putBeforeSave() {
        super.putBeforeSave();
        if (!currentDaip.isEmpty()) {
            put(CURRENTDAIP, currentDaip);
        }
        put(MINLEVEL, minLevel);
        put(MAXLEVEL, maxLevel);
        put(NBSUBNODES, nbSubNodes);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected boolean updated(final CouchbaseAccess dbvitam) {
        final ResultCached vt = (ResultCached) dbvitam.findOne(VitamCollections.Crequests, getId());
        if (vt != null) {
            final List<DBObject> list = new ArrayList<DBObject>();
            final List<String> slist = (List<String>) vt.get(CURRENTDAIP);
            if (slist != null) {
                final Set<String> newset = new HashSet<String>(currentDaip);
                newset.removeAll(currentDaip);
                if (!newset.isEmpty()) {
                    list.add(new BasicDBObject(CURRENTDAIP, new BasicDBObject("$each", newset)));
                }
            }
            if (!list.isEmpty()) {
                final BasicDBObject upd = new BasicDBObject();
                for (final DBObject dbObject : list) {
                    upd.putAll(dbObject);
                }
                upd.append(MINLEVEL, minLevel);
                upd.append(MAXLEVEL, maxLevel);
                upd.append(NBSUBNODES, nbSubNodes);
                final BasicDBObject update = new BasicDBObject("$addToSet", upd);
                // XXX FIXME cannot make an update
                //dbvitam.requests.collection.update(new BasicDBObject(ID, this.get(ID)), update);
            }
        }
        return false;
    }

    @Override
    public void save(final CouchbaseAccess dbvitam) {
        putBeforeSave();
        if (updated(dbvitam)) {
            return;
        }
        updateOrSave(dbvitam.requests);
        loaded = true;
    }

    @Override
    public void load(final CouchbaseAccess dbvitam) {
        final ResultCached vt = (ResultCached) dbvitam.findOne(VitamCollections.Crequests, getId());
        this.putAll(vt);
        loaded = true;
    }

    /**
     * Compute min and max from list of UUID in currentMaip.
     * Note: this should not be called from a list of "short" UUID, but only with "path" UUIDs
     */
    public void updateMinMax() {
        minLevel = 0;
        maxLevel = 0;
        for (final String id : currentDaip) {
            final int level = UUID.getUuidNb(id);
            if (minLevel > level) {
                minLevel = level;
            }
            if (maxLevel == 0 || maxLevel < level) {
                maxLevel = level;
            }
        }
    }

    /**
     * Compute min and max from list of real MAIP (from UUID), so loaded from database (could be heavy)
     *
     * @param dbvitam
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public void updateLoadMinMax(final CouchbaseAccess dbvitam) throws InstantiationException, IllegalAccessException {
        minLevel = 0;
        maxLevel = 0;
        for (final String id : currentDaip) {
            int level = UUID.getUuidNb(id);
            if (UUID.getUuidNb(id) == 1) {
                final DAip daip = DAip.findOne(dbvitam, id);
                if (daip == null) {
                    continue;
                }
                final Map<String, Integer> domdepth = daip.getDomDepth();
                if (domdepth == null || domdepth.isEmpty()) {
                    level = 1;
                } else {
                    level = Integer.MAX_VALUE;
                    for (final int lev : domdepth.values()) {
                        if (level > lev) {
                            level = lev;
                        }
                    }
                    level++;
                }
            }
            if (minLevel > level) {
                minLevel = level;
            }
            if (maxLevel == 0 || maxLevel < level) {
                maxLevel = level;
            }
        }
    }

    /**
     * @param mdAccess
     *            if null, this method returns always True (simulate)
     * @param next
     * @return True if this contains ancestors for next current
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public boolean checkAncestor(final CouchbaseAccess mdAccess, final ResultCached next) throws InstantiationException,
            IllegalAccessException {
        if (mdAccess == null) {
            return true;
        }
        final Set<String> previousLastSet = new HashSet<String>();
        // Compute last Id from previous result
        for (final String id : currentDaip) {
            previousLastSet.add(UUID.getLastAsString(id));
        }
        final Map<String, List<String>> nextFirstMap = new HashMap<String, List<String>>();
        // Compute first Id from current result
        for (final String id : next.currentDaip) {
            List<String> list = nextFirstMap.get(UUID.getFirstAsString(id));
            if (list == null) {
                list = new ArrayList<String>();
                nextFirstMap.put(UUID.getFirstAsString(id), list);
            }
            list.add(id);
        }
        final Map<String, List<String>> newMap = new HashMap<String, List<String>>(nextFirstMap);
        for (final String id : nextFirstMap.keySet()) {
            final DAip aip = DAip.findOne(mdAccess, id);
            if (aip == null) {
                continue;
            }
            final Map<String, Integer> fathers = aip.getDomDepth();
            final Set<String> fathersIds = fathers.keySet();
            // Check that parents of First Ids of Current result contains Last Ids from Previous result
            fathersIds.retainAll(previousLastSet);
            if (fathers.isEmpty()) {
                // issue there except if First = Last
                if (previousLastSet.contains(id)) {
                    continue;
                }
                newMap.remove(id);
            }
        }
        next.currentDaip.clear();
        for (final List<String> list : newMap.values()) {
            next.currentDaip.addAll(list);
        }
        next.putBeforeSave();
        return !next.currentDaip.isEmpty();
    }

    protected static void addIndexes(final CouchbaseAccess mongoDbAccess) {
        // dbvitam.requests.collection.createIndex(new BasicDBObject(MongoDbAccess.VitamLinks.DAip2PAip.field2to1, 1));
    }
}
