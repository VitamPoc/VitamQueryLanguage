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

import static fr.gouv.vitam.mdbtypes.MongoDbAccess.VitamCollections.Cdua;

import org.bson.BSONObject;

import com.mongodb.BasicDBObject;

/**
 * @author "Frederic Bregier"
 *
 */
public class DuaRef extends VitamType {
	
	private static final long serialVersionUID = -2179544540441187504L;

	public String name;
	public int duration;
	
	public DuaRef() {
		// empty 
	}

	/**
	 * @param name
	 * @param duration
	 */
	public DuaRef(String name, int duration) {
		super();
		this.name = name;
		this.duration = duration;
		putBeforeSave();
	}

	@Override
	public void getAfterLoad() {
		super.getAfterLoad();
		name = this.getString("name");
		duration = this.getInt("duration");
	}

	@Override
	public void putBeforeSave() {
		super.putBeforeSave();
		if (name != null) {
			this.put("name", name);
		}
		this.put("duration", duration);
	}
	
	@Override
	protected boolean updated(MongoDbAccess dbvitam) {
		return false;
	}

	public void save(MongoDbAccess dbvitam) {
		putBeforeSave();
		if (updated(dbvitam)) return;
		updateOrSave(dbvitam.duarefs);
	}
	@Override
	public void load(MongoDbAccess dbvitam) {
		DuaRef vt = (DuaRef) dbvitam.duarefs.collection.findOne(new BasicDBObject(ID, get(ID)));
		this.putAll((BSONObject) vt);
	}
	public static DuaRef findOne(MongoDbAccess dbvitam, String refid) throws InstantiationException, IllegalAccessException {
		return (DuaRef) dbvitam.findOne(Cdua, refid);
	}
}
