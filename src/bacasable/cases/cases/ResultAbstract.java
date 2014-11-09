/**
 * This file is part of Vitam Project.
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All Vitam Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * 
 * Vitam is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with Vitam . If not, see
 * <http://www.gnu.org/licenses/>.
 */

package fr.gouv.vitam.cases;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import fr.gouv.vitam.utils.UUID;

/**
 * @author "Frederic Bregier"
 *
 */
public abstract class ResultAbstract extends VitamType implements ResultInterface {
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
     * Default ID field name
     */
    public static final String ID = VitamType.ID;
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
     * Compute min and max from list of UUID in currentMaip.
     * Note: this should not be called from a list of "short" UUID, but only with "path" UUIDs
     */
    public final void updateMinMax() {
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
    }

    private static final DBObject FIELDDOMDEPTH = new BasicDBObject(DAip.DAIPDEPTHS, 1);

    /**
     * Compute min and max from list of real MAIP (from UUID), so loaded from database (could be heavy)
     *
     * @param dbvitam
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public final void updateLoadMinMax(final CassandraAccess dbvitam) throws InstantiationException, IllegalAccessException {
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
    public final boolean checkAncestor(final CassandraAccess mdAccess, final ResultInterface next) throws InstantiationException,
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
        final ResultAbstract rnext = (ResultAbstract) next;
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
    /**
     * @return the currentDaip
     */
    public final Set<String> getCurrentDaip() {
        return currentDaip;
    }
    /**
     * @param currentDaip the currentDaip to set
     */
    public final void setCurrentDaip(Set<String> currentDaip) {
        this.currentDaip = currentDaip;
    }
    /**
     * @return the minLevel
     */
    public final int getMinLevel() {
        return minLevel;
    }
    /**
     * @param minLevel the minLevel to set
     */
    public final void setMinLevel(int minLevel) {
        this.minLevel = minLevel;
    }
    /**
     * @return the maxLevel
     */
    public final int getMaxLevel() {
        return maxLevel;
    }
    /**
     * @param maxLevel the maxLevel to set
     */
    public final void setMaxLevel(int maxLevel) {
        this.maxLevel = maxLevel;
    }
    /**
     * @return the nbSubNodes
     */
    public final long getNbSubNodes() {
        return nbSubNodes;
    }
    /**
     * @param nbSubNodes the nbSubNodes to set
     */
    public final void setNbSubNodes(long nbSubNodes) {
        this.nbSubNodes = nbSubNodes;
    }
    /**
     * @return the loaded
     */
    public final boolean isLoaded() {
        return loaded;
    }
    /**
     * @param loaded the loaded to set
     */
    public final void setLoaded(boolean loaded) {
        this.loaded = loaded;
    }
    public String toString() {
        StringBuilder builder = new StringBuilder(this.getClass().getSimpleName());
        builder.append(ID);
        builder.append(':');
        builder.append(getId());
        builder.append(',');
        builder.append(MINLEVEL);
        builder.append(':');
        builder.append(minLevel);
        builder.append(',');
        builder.append(MAXLEVEL);
        builder.append(':');
        builder.append(maxLevel);
        builder.append(',');
        builder.append(NBSUBNODES);
        builder.append(':');
        builder.append(nbSubNodes);
        builder.append(',');
        builder.append("LOADED");
        builder.append(':');
        builder.append(loaded);
        builder.append(',');
        builder.append(CURRENTDAIP);
        builder.append(':');
        builder.append(currentDaip);
        return builder.toString();
    }
}
