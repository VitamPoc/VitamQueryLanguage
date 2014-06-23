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
package fr.gouv.vitam.cdbtypes;

import static fr.gouv.vitam.mdbtypes.MongoDbAccess.VitamCollections.Csaip;

import org.bson.BSONObject;

import com.mongodb.BasicDBObject;

/**
 * @author "Frederic Bregier"
 *
 */
public class SAip extends VitamType {
    
    private static final long serialVersionUID = -2179544540441187504L;
    
    public static enum StaticFields {
        storageId, storagePool, asyncAccess, nbCopy
    }

    
    public String storageId;
    public String storagePool;
    public boolean asyncAccess;
    public int nbCopy;
    
    public SAip() {
        // empty domaine
    }

    @Override
    public void getAfterLoad() {
        super.getAfterLoad();
        storageId = this.getString("storageId");
        storagePool = this.getString("storagePool");
        asyncAccess = this.getBoolean("asyncAccess");
        nbCopy = this.getInt("nbCopy");
    }
    @Override
    public void putBeforeSave() {
        super.putBeforeSave();
        if (storageId != null) {
            this.put("storageId", storageId);
        }
        if (storagePool != null) {
            this.put("storagePool", storagePool);
        }
        this.put("asyncAccess", asyncAccess);
        this.put("nbCopy", nbCopy);
    }

    @Override
    protected boolean updated(MongoDbAccess dbvitam) {
        return false;
    }

    @Override
    public void save(MongoDbAccess dbvitam) {
        putBeforeSave();
        if (updated(dbvitam)) return;
        updateOrSave(dbvitam.saips);
    }
    @Override
    public void load(MongoDbAccess dbvitam) {
        SAip vt = (SAip) dbvitam.saips.collection.findOne(new BasicDBObject(ID, get(ID)));
        this.putAll((BSONObject) vt);
    }
    public static SAip findOne(MongoDbAccess dbvitam, String refid) throws InstantiationException, IllegalAccessException {
        return (SAip) dbvitam.findOne(Csaip, refid);
    }
    public static void addIndexes(MongoDbAccess dbvitam) {
        dbvitam.daips.collection.createIndex(new BasicDBObject(MongoDbAccess.VitamLinks.PAip2SAip.field2to1, 1));
    }
}
