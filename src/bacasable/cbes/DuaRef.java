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

import static fr.gouv.vitam.cbes.CouchbaseAccess.VitamCollections.Cdua;

/**
 * DUA object
 *
 * @author "Frederic Bregier"
 *
 */
public class DuaRef extends VitamType {
    /**
     *
     */
    public static final String DURATION = "duration";
    /**
     *
     */
    public static final String NAME = "name";

    /**
     *
     */
    public String name;
    /**
     *
     */
    public int duration;

    /**
     *
     */
    public DuaRef() {
        // empty
    }

    /**
     * @param name
     * @param duration
     */
    public DuaRef(final String name, final int duration) {
        super();
        this.name = name;
        this.duration = duration;
        putBeforeSave();
    }

    @Override
    public void getAfterLoad() {
        super.getAfterLoad();
        name = this.getString(NAME);
        duration = this.getInt(DURATION);
    }

    @Override
    public void putBeforeSave() {
        super.putBeforeSave();
        if (name != null) {
            put(NAME, name);
        }
        put(DURATION, duration);
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
        updateOrSave(dbvitam.duarefs);
    }

    @Override
    public void load(final CouchbaseAccess dbvitam) {
        final DuaRef vt = (DuaRef) dbvitam.findOne(Cdua, getId());
        this.set(vt.json);
        this.getAfterLoad();
    }

    /**
     * @param dbvitam
     * @param refid
     * @return the DuaRef
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static DuaRef findOne(final CouchbaseAccess dbvitam, final String refid) throws InstantiationException,
            IllegalAccessException {
        return (DuaRef) dbvitam.findOne(Cdua, refid);
    }

    protected static void addIndexes(final CouchbaseAccess dbvitam) {
        // dbvitam.duarefs.collection.createIndex(new BasicDBObject(MongoDbAccess.VitamLinks.DAip2PAip.field2to1, 1));
    }

}
