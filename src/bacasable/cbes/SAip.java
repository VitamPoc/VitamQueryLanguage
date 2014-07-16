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
package fr.gouv.vitam.cbes;

import static fr.gouv.vitam.cbes.CouchbaseAccess.VitamCollections.Csaip;

import fr.gouv.vitam.cbes.CouchbaseAccess.VitamCollections;
import fr.gouv.vitam.cbes.CouchbaseAccess.VitamLinks;

/**
 * SAip object (Storage)
 *
 * @author "Frederic Bregier"
 *
 */
public class SAip extends VitamType {
    /**
     * Number of copies
     */
    public static final String NB_COPY = "nbCopy";
    /**
     * Is this access async
     */
    public static final String ASYNC_ACCESS = "asyncAccess";
    /**
     * Storage Pool
     */
    public static final String STORAGE_POOL = "storagePool";
    /**
     * Storage Id
     */
    public static final String STORAGE_ID = "storageId";

    /**
     * Storage Id
     */
    public String storageId;
    /**
     * Storage Pool
     */
    public String storagePool;
    /**
     * Is this access async
     */
    public boolean asyncAccess;
    /**
     * Number of copies
     */
    public int nbCopy;

    /**
     *
     */
    public SAip() {
        // empty domaine
    }

    @Override
    public void getAfterLoad() {
        super.getAfterLoad();
        storageId = this.getString(STORAGE_ID);
        storagePool = this.getString(STORAGE_POOL);
        asyncAccess = this.getBoolean(ASYNC_ACCESS);
        nbCopy = this.getInt(NB_COPY);
    }

    @Override
    public void putBeforeSave() {
        super.putBeforeSave();
        if (storageId != null) {
            put(STORAGE_ID, storageId);
        }
        if (storagePool != null) {
            put(STORAGE_POOL, storagePool);
        }
        put(ASYNC_ACCESS, asyncAccess);
        put(NB_COPY, nbCopy);
    }

    @Override
    protected boolean updated(final CouchbaseAccess dbvitam) {
        return false;
    }

    @Override
    public void save(final CouchbaseAccess dbvitam) {
        putBeforeSave();
        if (updated(dbvitam)) {
            return;
        }
        updateOrSave(dbvitam.saips);
    }

    @Override
    public void load(final CouchbaseAccess dbvitam) {
        final SAip vt = (SAip) dbvitam.findOne(Csaip, getId());
        this.putAll(vt);
    }

    /**
     * @param dbvitam
     * @param refid
     * @return the corresponding SAip
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static SAip findOne(final CouchbaseAccess dbvitam, final String refid) throws InstantiationException,
            IllegalAccessException {
        return (SAip) dbvitam.findOne(Csaip, refid);
    }

    /**
     * @param dbvitam
     */
    protected static void addIndexes(final CouchbaseAccess dbvitam) {
        CouchbaseAccess.executeCommand(dbvitam.saips.collection, 
                "CREATE INDEX "+VitamLinks.PAip2SAip.field2to1+"_IDX ON "+VitamCollections.Csaip.name+"("+VitamLinks.PAip2SAip.field2to1+")");
    }
}
