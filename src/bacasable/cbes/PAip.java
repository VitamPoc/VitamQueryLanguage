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

import static fr.gouv.vitam.cbes.CouchbaseAccess.VitamCollections.Cpaip;

import java.util.ArrayList;
import java.util.List;

import fr.gouv.vitam.cbes.CouchbaseAccess.VitamCollections;
import fr.gouv.vitam.cbes.CouchbaseAccess.VitamLinks;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * PAip object (Process)
 *
 * @author "Frederic Bregier"
 *
 */
public class PAip extends VitamType {
    /**
     * Example of Property
     */
    public static final String READING_MODE = "readingMode";

    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(PAip.class);

    /**
     * Example of Property
     */
    public String readingMode;

    /**
     *
     */
    public PAip() {
        // empty
    }

    @Override
    public void getAfterLoad() {
        super.getAfterLoad();
        readingMode = this.getString(READING_MODE);
    }

    @Override
    public void putBeforeSave() {
        super.putBeforeSave();
        if (readingMode != null) {
            put(READING_MODE, readingMode);
        }
    }

    @Override
    protected boolean updated(final CouchbaseAccess dbvitam) {
        final PAip vt = (PAip) dbvitam.findOne(Cpaip, getId());
        // XXX FIXME cannot do it
        /*
        BasicDBObject update = null;
        if (vt != null) {
            final List<DBObject> list = new ArrayList<>();
            BasicDBObject upd = dbvitam.updateLinks(this, vt, VitamLinks.DAip2PAip, false);
            if (upd != null) {
                list.add(upd);
            }
            upd = dbvitam.updateLink(this, vt, VitamLinks.PAip2Dua, true);
            if (upd != null) {
                list.add(upd);
            }
            if (!list.isEmpty()) {
                try {
                    update = new BasicDBObject();
                    if (!list.isEmpty()) {
                        upd = new BasicDBObject();
                        for (final DBObject dbObject : list) {
                            upd.putAll(dbObject);
                        }
                        update = update.append("$addToSet", upd);
                    }
                    dbvitam.paips.collection.update(new BasicDBObject(ID, this.get(ID)), update);
                } catch (final MongoException e) {
                    LOGGER.error("Exception for " + update, e);
                    throw e;
                }
                list.clear();
            }
            return true;
        } else {
            dbvitam.updateLinks(this, null, VitamLinks.DAip2PAip, false);
        }
        */
        return false;
    }

    @Override
    public void save(final CouchbaseAccess dbvitam) {
        putBeforeSave();
        if (updated(dbvitam)) {
            return;
        }
        updateOrSave(dbvitam.paips);
    }

    /**
     * @param remove
     * @return the list of parent DAip
     */
    @SuppressWarnings("unchecked")
    public List<String> getFathersDAipDBRef(final boolean remove) {
        if (remove) {
            return (List<String>) removeField(VitamLinks.DAip2PAip.field2to1);
        } else {
            return (List<String>) this.get(VitamLinks.DAip2PAip.field2to1);
        }
    }

    /**
     * Add the link N- between PAip and DuaRef
     *
     * @param dbvitam
     * @param dua
     */
    public void addDuaRef(final CouchbaseAccess dbvitam, final DuaRef dua) {
        dbvitam.addLink(this, VitamLinks.PAip2Dua, dua);
    }

    /**
     * @param remove
     * @return the list of DUA
     */
    @SuppressWarnings("unchecked")
    public List<String> getDuaRefDBRef(final boolean remove) {
        if (remove) {
            return (List<String>) removeField(VitamLinks.PAip2Dua.field1to2);
        } else {
            return (List<String>) this.get(VitamLinks.PAip2SAip.field1to2);
        }
    }

    @Override
    public void load(final CouchbaseAccess dbvitam) {
        final PAip vt = (PAip) dbvitam.findOne(Cpaip, getId());
        this.putAll(vt);
    }

    /**
     * @param dbvitam
     * @param refid
     * @return the PAip if found
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static PAip findOne(final CouchbaseAccess dbvitam, final String refid) throws InstantiationException,
            IllegalAccessException {
        return (PAip) dbvitam.findOne(Cpaip, refid);
    }

    protected static void addIndexes(final CouchbaseAccess dbvitam) {
        CouchbaseAccess.executeCommand(dbvitam.saips.collection, 
                "CREATE INDEX "+VitamLinks.DAip2PAip.field2to1+"_IDX ON "+VitamCollections.Cpaip.name+"("+VitamLinks.DAip2PAip.field2to1+")");
        CouchbaseAccess.executeCommand(dbvitam.saips.collection, 
                "CREATE INDEX "+VitamLinks.PAip2SAip.field1to2+"_IDX ON "+VitamCollections.Cpaip.name+"("+VitamLinks.PAip2SAip.field1to2+")");
        CouchbaseAccess.executeCommand(dbvitam.saips.collection, 
                "CREATE INDEX "+VitamLinks.PAip2Dua.field1to2+"_IDX ON "+VitamCollections.Cpaip.name+"("+VitamLinks.PAip2Dua.field1to2+")");
    }
}
