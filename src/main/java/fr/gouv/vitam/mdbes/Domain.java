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

import static fr.gouv.vitam.mdbes.MongoDbAccess.VitamCollections.Cdomain;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import fr.gouv.vitam.mdbes.MongoDbAccess.VitamLinks;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;


/**
 * @author "Frederic Bregier"
 *
 */
public class Domain extends VitamType {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(Domain.class);
    
    private static final long serialVersionUID = 8152306914666919955L;
    
    /**
     * Number of Immediate child (DAip)
     */
    public static final String NBCHILD = "_nb";
    
    /**
     * Number of Immediate child (DAip)
     */
    public long nb = 0;
    
    public Domain() {
        // empty 
    }

    /**
     * Add the link N-N between Domain and List of DAip
     * @param dbvitam
     * @param maips
     */
    public void addMetaAip(MongoDbAccess dbvitam, List<DAip> maips) {
        DBObject update = null;
        List<String> ids = new ArrayList<String>();
        for (DAip maip : maips) {
            DBObject update2 = dbvitam.addLink(this, VitamLinks.Domain2DAip, maip);
            if (update2 != null) {
                update = update2;
                ids.add((String) maip.get(ID));
                //maip.update(dbvitam.metaaips, update);
            }
        }
        if (! ids.isEmpty()) {
            if (ids.size() > 1) {
                try {
                    dbvitam.daips.collection.update(new BasicDBObject(ID, new BasicDBObject("$in", ids)), update, false, true);
                } catch (MongoException e) {
                    LOGGER.error("Exception for " + update, e);
                    throw e;
                }
            } else {
                try {
                    dbvitam.daips.collection.update(new BasicDBObject(ID, ids.get(0)), update);
                } catch (MongoException e) {
                    LOGGER.error("Exception for " + update, e);
                    throw e;
                }
            }
            nb += ids.size();
        }
        ids.clear();
    }

    /**
     * Add the link N-N between Domain and List of DAip
     * @param dbvitam
     * @param maips
     */
    public void addMetaAipNoSave(MongoDbAccess dbvitam, OutputStream outputStream, List<DAip> maips) {
        for (DAip maip : maips) {
            MongoDbAccess.addAsymmetricLinksetNoSave(this, VitamLinks.Domain2DAip.field1to2, maip, false);
            if (MongoDbAccess.addAsymmetricLinksetNoSave(maip, VitamLinks.Domain2DAip.field2to1, this, false)) {
                nb++;
            }
            maip.saveToFile(dbvitam, outputStream, 1);
        }
    }

    /**
     * Add the link N-N between Domain and DAip
     * @param dbvitam
     * @param maip
     */
    public void addMetaAip(MongoDbAccess dbvitam, DAip maip) {
        DBObject update = dbvitam.addLink(this, VitamLinks.Domain2DAip, maip);
        if (update != null) {
            maip.update(dbvitam.daips, update);
        }
    }

    @Override
    protected boolean updated(MongoDbAccess dbvitam) {
        Domain vt = (Domain) dbvitam.domains.collection.findOne(new BasicDBObject(ID, (String) get(ID)));
        BasicDBObject update = null;
        if (vt != null) {
            List<DBObject> list = new ArrayList<>();
            BasicDBObject upd = dbvitam.updateLinks(this, vt, VitamLinks.Domain2DAip, true);
            if (upd != null){
                list.add(upd);
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
                update = update.append("$inc", new BasicDBObject(NBCHILD, nb));
                nb = 0;
                dbvitam.domains.collection.update(new BasicDBObject(ID, this.get(ID)), update);
            } catch (MongoException e) {
                LOGGER.error("Exception for {}", update, e);
                throw e;
            }
            list.clear();
            return true;
        } else {
            dbvitam.updateLinks(this, null, VitamLinks.Domain2DAip, true);
            this.append(NBCHILD, nb);
            nb = 0;
        }
        return false;
    }

    public void save(MongoDbAccess dbvitam) {
        putBeforeSave();
        if (updated(dbvitam)) {
            return;
        }
        updateOrSave(dbvitam.domains);
    }
    @SuppressWarnings("unchecked")
    public List<String> getMetaAipDBRef(boolean remove) {
        if (remove) {
            return (List<String>) this.removeField(VitamLinks.Domain2DAip.field1to2);
        } else {
            return (List<String>) this.get(VitamLinks.Domain2DAip.field1to2);
        }
    }
    
    public final void cleanStructure() {
        removeField(VitamLinks.Domain2DAip.field1to2);
        removeField(ID);
        removeField("_refid");
        removeField("_nb");
    }
    
    @Override
    public void load(MongoDbAccess dbvitam) {
        Domain vt = (Domain) dbvitam.domains.collection.findOne(new BasicDBObject(ID, get(ID)));
        this.putAll((BSONObject) vt);
    }

    public static Domain findOne(MongoDbAccess dbvitam, String id) throws InstantiationException, IllegalAccessException {
        return (Domain) dbvitam.findOne(Cdomain, id);
    }
    public static void addIndexes(MongoDbAccess dbvitam) {
        dbvitam.domains.collection.createIndex(new BasicDBObject(MongoDbAccess.VitamLinks.Domain2DAip.field1to2, 1));
        //dbvitam.domaines.collection.createIndex(new BasicDBObject("_depth", 1));
    }
}
