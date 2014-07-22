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
package fr.gouv.vitam.mdbes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BSONObject;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.UUID;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * Result (potentially cached) object
 *
 * @author "Frederic Bregier"
 *
 */
public class ResultMongodbBak extends VitamType implements ResultInterface {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(ResultMongodbBak.class);
    private static final long serialVersionUID = 5962911495483495562L;

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
     * TTL
     */
    public static final String TTL = "__ttl";

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
     * ttl date
     */
    private Date ttl = getNewTtl();
    
    private static final Date getNewTtl() {
        return new Date(System.currentTimeMillis()+GlobalDatas.TTLMS);
    }
    /**
     *
     */
    public ResultMongodbBak() {

    }

    /**
     * @param collection
     */
    public ResultMongodbBak(final Collection<String> collection) {
        currentDaip.addAll(collection);
        updateMinMax();
        // Path list so as loaded (never cached)
        loaded = true;
        putBeforeSave();
    }
    /**
     * Put from argument
     * @param from
     */
    public void putFrom(final ResultInterface from) {
        this.putAll((BSONObject) from);
        loaded = true;
        getAfterLoad();
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

    @SuppressWarnings("unchecked")
    @Override
    public void getAfterLoad() {
        super.getAfterLoad();
        if (containsField(CURRENTDAIP)) {
            final Object obj = this.get(CURRENTDAIP);
            final Set<String> vtset = new HashSet<String>();
            if (obj instanceof BasicDBList) {
                for (Object string : (BasicDBList) obj) {
                    vtset.add((String) string);
                }
            } else {
                vtset.addAll((Set<String>) obj);
            }
            currentDaip.clear();
            currentDaip.addAll(vtset);
        }
        minLevel = this.getInt(MINLEVEL, 0);
        maxLevel = this.getInt(MAXLEVEL, 0);
        nbSubNodes = this.getLong(NBSUBNODES, -1);
        ttl = this.getDate(TTL, getNewTtl());
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
        put(TTL, ttl);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected boolean updated(final MongoDbAccess dbvitam) {
        final ResultMongodbBak vt = (ResultMongodbBak) dbvitam.requests.collection.findOne(getId());
        if (vt != null) {
            final List<DBObject> list = new ArrayList<DBObject>();
            final Object obj = vt.get(CURRENTDAIP);
            final Set<String> vtset = new HashSet<String>();
            if (obj instanceof BasicDBList) {
                for (Object string : (BasicDBList) obj) {
                    vtset.add((String) string);
                }
            } else {
                vtset.addAll((Set<String>) obj);
            }
            if (! vtset.isEmpty()) {
                final Set<String> newset = new HashSet<String>(currentDaip);
                newset.removeAll(vtset);
                if (!newset.isEmpty()) {
                    list.add(new BasicDBObject(CURRENTDAIP, new BasicDBObject("$each", newset)));
                }
            }
            if (!list.isEmpty()) {
                final BasicDBObject updset = new BasicDBObject();
                for (final DBObject dbObject : list) {
                    updset.putAll(dbObject);
                }
                final BasicDBObject upd = new BasicDBObject();
                upd.append(MINLEVEL, minLevel);
                upd.append(MAXLEVEL, maxLevel);
                upd.append(NBSUBNODES, nbSubNodes);
                upd.append(TTL, ttl);
                final BasicDBObject update = new BasicDBObject("$addToSet", updset).
                        append("$set", upd);
                dbvitam.requests.collection.update(new BasicDBObject(ID, this.get(ID)), update);
            }
            if (GlobalDatas.PRINT_REQUEST) {
                LOGGER.warn("UPDATE: "+this);
            }
        } else if (containsField(ID)) {
            // not in DB but got already an ID => Save it
            if (GlobalDatas.PRINT_REQUEST) {
                LOGGER.warn("SAVE: "+this);
            }
            this.forceSave(dbvitam.requests);
            return true;
        }
        return false;
    }

    @Override
    public void save(final MongoDbAccess dbvitam) {
        putBeforeSave();
        if (updated(dbvitam)) {
            loaded = true;
            return;
        }
        updateOrSave(dbvitam.requests);
        loaded = true;
    }

    @Override
    public boolean load(final MongoDbAccess dbvitam) {
        final ResultMongodbBak vt = (ResultMongodbBak) dbvitam.requests.collection.findOne(getId());
        if (vt == null) {
            return false;
        }
        this.putAll((BSONObject) vt);
        getAfterLoad();
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("LOAD: "+this);
        }
        loaded = true;
        return true;
    }
    /**
     * Update the TTL for this
     * @param dbvitam
     */
    public void updateTtl(final MongoDbAccess dbvitam) {
        ttl = getNewTtl();
        dbvitam.requests.collection.update(new BasicDBObject(ID, this.get(ID)), 
                new BasicDBObject().append("$set", new BasicDBObject(TTL, ttl)));
    }
    /**
     * Compute min and max from list of UUID in currentMaip.
     * Note: this should not be called from a list of "short" UUID, but only with "path" UUIDs
     */
    public void updateMinMax() {
        minLevel = 0;
        maxLevel = 0;
        if (currentDaip.isEmpty()) {
            return;
        }
        minLevel = Integer.MAX_VALUE;
        for (final String id : currentDaip) {
            final int level = UUID.getUuidNb(id);
            if (minLevel > level) {
                minLevel = level;
            }
            if (maxLevel == 0 || maxLevel < level) {
                maxLevel = level;
            }
        }
        if (GlobalDatas.PRINT_REQUEST) {
            LOGGER.warn("min: "+minLevel+" max: "+maxLevel);
        }
    }

    private static final DBObject FIELDDOMDEPTH = new BasicDBObject(DAip.DAIPDEPTHS, 1);

    /**
     * Compute min and max from list of real MAIP (from UUID), so loaded from database (could be heavy)
     *
     * @param dbvitam
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public void updateLoadMinMax(final MongoDbAccess dbvitam) throws InstantiationException, IllegalAccessException {
        minLevel = 0;
        maxLevel = 0;
        if (currentDaip.isEmpty()) {
            return;
        }
        minLevel = Integer.MAX_VALUE;
        for (final String id : currentDaip) {
            int level = UUID.getUuidNb(id);
            if (level == 1) {
                DBObject dbObject = dbvitam.daips.collection.findOne(id, FIELDDOMDEPTH);
                if (dbObject == null) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                final Map<String, Integer> domdepth = (Map<String, Integer>) dbObject.get(DAip.DAIPDEPTHS);
                /*
                final DAip daip = DAip.findOne(dbvitam, id);
                if (daip == null) {
                    continue;
                }
                final Map<String, Integer> domdepth = daip.getDomDepth();
                 */
                if (domdepth == null || domdepth.isEmpty()) {
                    level = 1;
                } else {
                    level = 0;
                    for (final int lev : domdepth.values()) {
                        if (level < lev) {
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
            if (GlobalDatas.PRINT_REQUEST) {
                LOGGER.warn("min: "+minLevel+" max: "+maxLevel);
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
    public boolean checkAncestor(final MongoDbAccess mdAccess, final ResultInterface next) throws InstantiationException,
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
        final ResultMongodbBak rnext = (ResultMongodbBak) next;
        // Compute first Id from current result
        for (final String id : rnext.currentDaip) {
            List<String> list = nextFirstMap.get(UUID.getFirstAsString(id));
            if (list == null) {
                list = new ArrayList<String>();
                nextFirstMap.put(UUID.getFirstAsString(id), list);
            }
            list.add(id);
        }
        final Map<String, List<String>> newMap = new HashMap<String, List<String>>(nextFirstMap);
        for (final String id : nextFirstMap.keySet()) {
            DBObject dbObject = mdAccess.daips.collection.findOne(id, FIELDDOMDEPTH);
            if (dbObject == null) {
                continue;
            }
            @SuppressWarnings("unchecked")
            final Map<String, Integer> fathers = (Map<String, Integer>) dbObject.get(DAip.DAIPDEPTHS);
            /*
            final DAip aip = DAip.findOne(mdAccess, id);
            if (aip == null) {
                continue;
            }
            final Map<String, Integer> fathers = aip.getDomDepth();
            */
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
        rnext.currentDaip.clear();
        for (final List<String> list : newMap.values()) {
            rnext.currentDaip.addAll(list);
        }
        rnext.putBeforeSave();
        return !rnext.currentDaip.isEmpty();
    }

    protected static void addIndexes(final MongoDbAccess mongoDbAccess) {
        // dbvitam.requests.collection.createIndex(new BasicDBObject(MongoDbAccess.VitamLinks.DAip2PAip.field2to1, 1));
        mongoDbAccess.requests.collection.createIndex(new BasicDBObject(TTL, 1), new BasicDBObject("expireAfterSeconds", 0));
    }
    /**
     * @return the currentDaip
     */
    public Set<String> getCurrentDaip() {
        return currentDaip;
    }
    /**
     * @param currentDaip the currentDaip to set
     */
    public void setCurrentDaip(Set<String> currentDaip) {
        this.currentDaip = currentDaip;
    }
    /**
     * @return the minLevel
     */
    public int getMinLevel() {
        return minLevel;
    }
    /**
     * @param minLevel the minLevel to set
     */
    public void setMinLevel(int minLevel) {
        this.minLevel = minLevel;
    }
    /**
     * @return the maxLevel
     */
    public int getMaxLevel() {
        return maxLevel;
    }
    /**
     * @param maxLevel the maxLevel to set
     */
    public void setMaxLevel(int maxLevel) {
        this.maxLevel = maxLevel;
    }
    /**
     * @return the nbSubNodes
     */
    public long getNbSubNodes() {
        return nbSubNodes;
    }
    /**
     * @param nbSubNodes the nbSubNodes to set
     */
    public void setNbSubNodes(long nbSubNodes) {
        this.nbSubNodes = nbSubNodes;
    }
    /**
     * @return the loaded
     */
    public boolean isLoaded() {
        return loaded;
    }
    /**
     * @param loaded the loaded to set
     */
    public void setLoaded(boolean loaded) {
        this.loaded = loaded;
    }
}
