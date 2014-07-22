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

package fr.gouv.vitam.mdbes;

import java.util.Set;

/**
 * @author "Frederic Bregier"
 *
 */
public interface ResultInterface {

    /**
     * Set a new ID
     *
     * @param id
     */
    public void setId(final String id);
    /**
    *
    * @return the ID
    */
   public String getId();
   
   /**
    * Put from argument
    * @param from
    */
   public void putFrom(final ResultInterface from);
   /**
    * To be called after a load from Database
    */
   public void getAfterLoad();
   /**
    * To be called before save to database
    */
   public void putBeforeSave();
   /**
    * Save to the Couchbase Database
    * @param dbvitam
    */
   public void save(final MongoDbAccess dbvitam);
   /**
    * Update the TTL for this
    * @param dbvitam
    */
   public void updateTtl(final MongoDbAccess dbvitam);
   /**
    * Compute min and max from list of UUID in currentMaip.
    * Note: this should not be called from a list of "short" UUID, but only with "path" UUIDs
    */
   public void updateMinMax();
   /**
    * Compute min and max from list of real MAIP (from UUID), so loaded from database (could be heavy)
    *
    * @param dbvitam
    * @throws IllegalAccessException
    * @throws InstantiationException
    */
   public void updateLoadMinMax(final MongoDbAccess dbvitam) throws InstantiationException, IllegalAccessException;
   /**
    * @param mdAccess
    *            if null, this method returns always True (simulate)
    * @param next
    * @return True if this contains ancestors for next current
    * @throws InstantiationException
    * @throws IllegalAccessException
    */
   public boolean checkAncestor(final MongoDbAccess mdAccess, final ResultInterface next) throws InstantiationException,
           IllegalAccessException;
   /**
    * @return the currentDaip
    */
   public Set<String> getCurrentDaip();
   /**
    * @param currentDaip the currentDaip to set
    */
   public void setCurrentDaip(Set<String> currentDaip);
   /**
    * @return the minLevel
    */
   public int getMinLevel();
   /**
    * @param minLevel the minLevel to set
    */
   public void setMinLevel(int minLevel);
   /**
    * @return the maxLevel
    */
   public int getMaxLevel();
   /**
    * @param maxLevel the maxLevel to set
    */
   public void setMaxLevel(int maxLevel);
   /**
    * @return the nbSubNodes
    */
   public long getNbSubNodes();
   /**
    * @param nbSubNodes the nbSubNodes to set
    */
   public void setNbSubNodes(long nbSubNodes);
   /**
    * @return the loaded status
    */
   public boolean isLoaded();
   /**
    * @param loaded the loaded status to set
    */
   public void setLoaded(boolean loaded);
}
