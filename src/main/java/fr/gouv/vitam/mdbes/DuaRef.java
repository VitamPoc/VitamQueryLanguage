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
package fr.gouv.vitam.mdbes;

import static fr.gouv.vitam.mdbes.MongoDbAccess.VitamCollections.Cdua;

import org.bson.BSONObject;

import com.mongodb.BasicDBObject;

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

    private static final long serialVersionUID = -2179544540441187504L;

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
    protected boolean updated(final MongoDbAccess dbvitam) {
        return false;
    }

    @Override
    public void save(final MongoDbAccess dbvitam) {
        putBeforeSave();
        if (updated(dbvitam)) {
            return;
        }
        updateOrSave(dbvitam.duarefs);
    }

    @Override
    public void load(final MongoDbAccess dbvitam) {
        final DuaRef vt = (DuaRef) dbvitam.duarefs.collection.findOne(new BasicDBObject(ID, get(ID)));
        this.putAll((BSONObject) vt);
    }

    /**
     * @param dbvitam
     * @param refid
     * @return the DuaRef
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static DuaRef findOne(final MongoDbAccess dbvitam, final String refid) throws InstantiationException,
            IllegalAccessException {
        return (DuaRef) dbvitam.findOne(Cdua, refid);
    }

    protected static void addIndexes(final MongoDbAccess dbvitam) {
        // dbvitam.duarefs.collection.createIndex(new BasicDBObject(MongoDbAccess.VitamLinks.DAip2PAip.field2to1, 1));
    }

}
