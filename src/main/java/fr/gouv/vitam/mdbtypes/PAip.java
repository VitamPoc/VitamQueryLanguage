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
package fr.gouv.vitam.mdbtypes;

import static fr.gouv.vitam.mdbtypes.MongoDbAccess.VitamCollections.Cpaip;

import java.util.ArrayList;
import java.util.List;

import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import fr.gouv.vitam.mdbtypes.MongoDbAccess.VitamLinks;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;


/**
 * @author "Frederic Bregier"
 *
 */
public class PAip extends VitamType {
	private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(PAip.class);
	
    private static final long serialVersionUID = -2179544540441187504L;
    
    public static enum StaticFields {
        readingMode
    }
    /**
     * Example of Property
     */
    public String readingMode;
    
    public PAip() {
        // empty 
    }
    
    @Override
    public void getAfterLoad() {
        super.getAfterLoad();
        readingMode = this.getString("readingMode");
    }
    @Override
    public void putBeforeSave() {
        super.putBeforeSave();
        if (readingMode != null) {
            this.put("readingMode", readingMode);
        }
    }
    
    @Override
    protected boolean updated(MongoDbAccess dbvitam) {
        PAip vt = (PAip) dbvitam.paips.collection.findOne(new BasicDBObject(ID, get(ID)));
        BasicDBObject update = null;
        if (vt != null) {
            List<DBObject> list = new ArrayList<>();
            BasicDBObject upd = dbvitam.updateLinks(this, vt, VitamLinks.DAip2PAip, false);
            if (upd != null){
                list.add(upd);
            }
            upd = dbvitam.updateLink(this, vt, VitamLinks.PAip2Dua, true);
            if (upd != null) { 
                list.add(upd);
            }
            if (! list.isEmpty()) {
                try {
                    update = new BasicDBObject();
                    if (!list.isEmpty()) {
                        upd = new BasicDBObject();
                        for (DBObject dbObject : list) {
                            upd.putAll(dbObject);
                        }
                        update = update.append("$addToSet", upd);
                    }
                    dbvitam.paips.collection.update(new BasicDBObject(ID, this.get(ID)), update);
                } catch (MongoException e) {
                	LOGGER.error("Exception for " + update, e);
                    throw e;
                }
                list.clear();
            }
            return true;
        } else {
            dbvitam.updateLinks(this, null, VitamLinks.DAip2PAip, false);
        }
        return false;
    }
    
    @Override
    public void save(MongoDbAccess dbvitam) {
        putBeforeSave();
        if (updated(dbvitam)) return;
        updateOrSave(dbvitam.paips);
    }
    @SuppressWarnings("unchecked")
    public List<String> getFathersMetaAipDBRef(boolean remove) {
        if (remove) {
            return (List<String>) this.removeField(VitamLinks.DAip2PAip.field2to1);
        } else {
            return (List<String>) this.get(VitamLinks.DAip2PAip.field2to1);
        }
    }
    /**
     * Add the link N- between PAip and DuaRef
     * @param dbvitam
     * @param dua
     */
    public void addDuaRef(MongoDbAccess dbvitam, DuaRef dua) {
        dbvitam.addLink(this, VitamLinks.PAip2Dua, dua);
    }
    @SuppressWarnings("unchecked")
    public List<String> getDuaDBRef(boolean remove) {
        if (remove) {
            return (List<String>) this.removeField(VitamLinks.PAip2Dua.field1to2);
        } else {
            return (List<String>) this.get(VitamLinks.PAip2SAip.field1to2);
        }
    }
    @Override
    public void load(MongoDbAccess dbvitam) {
        PAip vt = (PAip) dbvitam.paips.collection.findOne(new BasicDBObject(ID, get(ID)));
        this.putAll((BSONObject) vt);
    }
    
    public static PAip findOne(MongoDbAccess dbvitam, String refid) throws InstantiationException, IllegalAccessException {
        return (PAip) dbvitam.findOne(Cpaip, refid);
    }
    public static void addIndexes(MongoDbAccess dbvitam) {
        dbvitam.daips.collection.createIndex(new BasicDBObject(MongoDbAccess.VitamLinks.DAip2PAip.field2to1, 1));
        dbvitam.daips.collection.createIndex(new BasicDBObject(MongoDbAccess.VitamLinks.PAip2SAip.field1to2, 1));
        dbvitam.daips.collection.createIndex(new BasicDBObject(MongoDbAccess.VitamLinks.PAip2Dua.field1to2, 1));
    }
}
