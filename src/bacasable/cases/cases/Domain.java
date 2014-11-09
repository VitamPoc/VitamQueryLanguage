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
package fr.gouv.vitam.cases;

import static fr.gouv.vitam.cases.CassandraAccess.VitamCollections.Cdomain;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import fr.gouv.vitam.cases.CassandraAccess.VitamLinks;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * Domain object
 *
 * @author "Frederic Bregier"
 *
 */
public class Domain extends VitamType {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(Domain.class);

    /**
     * Number of Immediate child (DAip)
     */
    public static final String NBCHILD = "_nb";

    protected static final String[] fields = { NBCHILD, VitamLinks.Domain2DAip.field1to2 };
    
    /**
     *
     */
    public Domain() {
        // empty
    }

    /**
     * Add the link N-N between Domain and List of DAip
     *
     * @param dbvitam
     * @param maips
     */
    public void addDAip(final CassandraAccess dbvitam, final List<DAip> maips) {
        final List<String> ids = new ArrayList<String>();
        for (final DAip maip : maips) {
            String update2 = dbvitam.addLink(this, VitamLinks.Domain2DAip, maip);
            if (update2 != null) {
                ids.add(update2);
                // maip.update(dbvitam.metaaips, update);
            }
        }
        if (!ids.isEmpty()) {
            String []keys = { VitamLinks.Domain2DAip.field2to1 };
            Set<String> set = new HashSet<String>();
            set.add(this.id);
            Object []values = { set };
            for (String string : ids) {
                dbvitam.daips.update(string, keys, values);
            }
            nb += ids.size();
        }
        ids.clear();
    }

    /**
     * Add the link N-N between Domain and List of DAip (version save to file)
     *
     * @param dbvitam
     * @param outputStream
     * @param maips
     */
    public void addDAipNoSave(final CassandraAccess dbvitam, final OutputStream outputStream, final List<DAip> maips) {
        for (final DAip maip : maips) {
            addDaipsAsymmetricLinksetNoSave(this, maip);
            if (addDomsAsymmetricLinksetNoSave(maip, this)) {
                nb++;
            }
            maip.saveToFile(dbvitam, outputStream);
        }
    }

    /**
     * Add the link N-N between Domain and DAip
     *
     * @param dbvitam
     * @param maip
     */
    public void addDAip(final CassandraAccess dbvitam, final DAip maip) {
        String update = dbvitam.addLink(this, VitamLinks.Domain2DAip, maip);
        if (update != null) {
            String []keys = { VitamLinks.Domain2DAip.field2to1 };
            Set<String> set = new HashSet<String>();
            set.add(this.id);
            Object []values = { set };
            dbvitam.daips.update(maip.id, keys, values);
        }
    }

    @Override
    protected boolean updated(final CassandraAccess dbvitam) {
        Domain vt;
        try {
            vt = (Domain) dbvitam.domains.getObject(id);
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            updateDaipsLinks(null);
            return false;
        }
        LOGGER.debug("Previous Domain exists ? " + (vt != null));
        if (vt != null) {
            final List<String> listKey = new ArrayList<>();
            final List<Object> listValue = new ArrayList<Object>();
            Set<String> upd = this.updateDaipsLinks(vt);
            if (upd != null) {
                listKey.add(VitamLinks.Domain2DAip.field1to2);
                listValue.add(upd);
            }
            listKey.add(NBCHILD);
            listValue.add(nb+vt.nb);
            nb = 0;
            dbvitam.domains.update(id, (String[]) listKey.toArray(), listValue.toArray());
            listKey.clear();
            listValue.clear();
            return true;
        } else {
            updateDaipsLinks(null);
        }
        return false;
    }

    @Override
    public void save(final CassandraAccess dbvitam) {
        if (updated(dbvitam)) {
            return;
        }
        LOGGER.debug("Domain will be saved: {}", this);
        updateOrSave(dbvitam.domains);
        nb = 0;
    }

    /**
     * @param remove
     * @return the list of DAip
     */
    @SuppressWarnings("unchecked")
    public Set<String> getDAipDBRef(final boolean remove) {
        if (remove) {
            Set<String> oldset = daips;
            daips = null;
            return oldset;
        } else {
            return daips;
        }
    }

    /**
     * Used in loop to clean the object
     */
    public final void cleanStructure() {
        daips = null;
        id = null;
        nb = 0;
        //removeField("_refid");
    }

    @Override
    public boolean load(final CassandraAccess dbvitam) {
        Domain vt;
        try {
            vt = (Domain) dbvitam.domains.getObject(id);
        } catch (InstantiationException | IllegalAccessException e) {
            return false;
        }
        if (vt == null) {
            return false;
        }
        return true;
    }

    @Override
    protected String[] getSpecificKeys() {
        return fields;
    }


    @Override
    protected void setFromResultSetRow(Row row) {
        super.setFromResultSetRow(row);
        nb = row.getLong(NBCHILD);
        daips = row.getSet(VitamLinks.Domain2DAip.field1to2, String.class);
    }

    /**
     * @param dbvitam
     * @param id
     * @return the Domain
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static Domain findOne(final CassandraAccess dbvitam, final String id) throws InstantiationException,
            IllegalAccessException {
        return (Domain) dbvitam.domains.getObject(id);
    }

    protected static void addIndexes(final CassandraAccess dbvitam) {
        dbvitam.domains.createIndex(CassandraAccess.VitamLinks.Domain2DAip.field1to2);
        // dbvitam.domaines.collection.createIndex(new BasicDBObject("_depth", 1));
    }
    protected static String createTable() {
        return ", "+CassandraAccess.VitamLinks.Domain2DAip.field1to2+ " set<varchar>"+
                ", "+NBCHILD+ " bigint";
    }

}
