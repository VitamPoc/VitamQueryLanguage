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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.Row;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.cases.CassandraAccess.VitamCollection;
import fr.gouv.vitam.query.GlobalDatas;
import fr.gouv.vitam.utils.UUID;
import fr.gouv.vitam.utils.exception.InvalidParseOperationException;
import fr.gouv.vitam.utils.exception.InvalidUuidOperationException;
import fr.gouv.vitam.utils.json.JsonHandler;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * The default Vitam Type object to be stored in the database (Cassandra/ElasticSearch mode)
 *
 * @author "Frederic Bregier"
 *
 */
public abstract class VitamType extends ObjectNode {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(VitamType.class);

    /**
     * Default ID field name
     */
    public static final String ID = "_id";
    /**
     * Default JSON field name
     */
    public static final String JSONFIELD = "_json";
    
    protected String id;
    /**
     * Number of Immediate child (DAip)
     */
    public long nb = 0;

    /**
     * Immediate Sons of Domain
     */
    protected Set<String> daips;
    /**
     * Immediate father of Daips as Domains
     */
    protected Set<String> doms;
    /**
     * Immediate fathers
     */
    protected Set<String> up;
    /**
     * Immediate link with Paip
     */
    protected String paip;
    /**
     * Immediate link with Saip
     */
    protected String saip;
    /**
     * Immediate links with Dua
     */
    protected Set<String> dua;
    /**
     * All fathers (up to Domain) with associated min level
     */
    protected Map<String, Integer> dps;
    /**
     * All fathers (up to Domain)
     */
    protected Set<String> dds;
    

    /**
     * Empty constructor
     */
    public VitamType() {
        super(JsonHandler.getFactory());
    }

    /**
     * This (Domain) is a root
     *
     * @throws InvalidUuidOperationException
     */
    public final void setRoot() throws InvalidUuidOperationException {
        if (id == null) {
            id = new UUID().toString();
        }
        GlobalDatas.ROOTS.add(id);
    }

    /**
     * Create a new ID
     */
    public final void setNewId() {
        id = new UUID().toString();
    }

    /**
     * Set a new ID
     *
     * @param id
     */
    public final void setId(final String id) {
        this.id = id;
    }

    /**
     *
     * @return the ID
     */
    public String getId() {
        return id;
    }

    /**
     * Load from a JSON String
     *
     * @param json
     */
    public void load(final String json) {
        try {
            ObjectNode on = (ObjectNode) JsonHandler.getFromString(json);
            load(on);
        } catch (InvalidParseOperationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Load from a ObjectNode
     * @param on
     */
    public void load(ObjectNode on) {
        id = on.remove(ID).asText();
        this.setAll(on);
    }

    /**
     * Setter from a Row
     * @param row
     */
    protected void setFromResultSetRow(Row row) {
        id = row.getString(ID);
        try {
            this.setAll((ObjectNode)JsonHandler.getFromString(row.getString(JSONFIELD)));
        } catch (InvalidParseOperationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    /**
     * Save the object. Implementation should call putBeforeSave before the real save operation
     *
     * @param dbvitam
     */
    public abstract void save(CassandraAccess dbvitam);

    /**
     * try to update the object if necessary (difference from the current value in the database)
     *
     * @param dbvitam
     * @return True if the object does not need any extra save operation
     */
    protected abstract boolean updated(CassandraAccess dbvitam);

    /**
     * load the object from the database, ignoring any previous data, except ID
     *
     * @param dbvitam
     * @return True if the object is loaded
     */
    public abstract boolean load(CassandraAccess dbvitam);

    /**
     * 
     * @return the array of specific key names that will be saved as columns, the others will be in "json" column
     */
    protected abstract String[] getSpecificKeys();
    /**
     * Save the document if new, update it (keeping non set fields, replacing set fields)
     *
     * @param collection
     */
    protected final void updateOrSave(final VitamCollection collection) {
        if (id == null) {
            setNewId();
            collection.save(this, 0);
        } else {
            collection.update(this, 0);
        }
    }
    
    /**
     * Force the save (insert) of this document (no putBeforeSave done)
     * @param collection
     */
    protected final void forceSave(final VitamCollection collection) {
        if (id == null) {
            setNewId();
        }
        collection.save(this, 0);
    }
    /**
     * Delete the current object
     *
     * @param collection
     */
    protected final void delete(final VitamCollection collection) {
        collection.delete(id);
    }

    /**
     * Update the item (must be saved before) using the update part
     *
     * @param collection
     * @param update
     */
    protected final void update(final VitamCollection collection, final String[] fields, final Object[] values) {
        collection.update(this.getId(), fields, values);
    }
    /**
     *
     * @return the bypass toString
     */
    public String toStringDirect() {
        return super.toString();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ": " + super.toString();
    }

    /**
     *
     * @return the toString for Debug mode
     */
    public String toStringDebug() {
        return this.getClass().getSimpleName() + ": " + id;
    }

    /**
     * Update the link paip
     *
     * @param vtReloaded
     * @return the update part if needed
     */
    protected final String updatePaip(final VitamType vtReloaded) {
        if (vtReloaded != null) {
            String srcOid = vtReloaded.paip;
            final String targetOid = paip;
            if (srcOid != null && targetOid != null) {
                if (targetOid.equals(srcOid)) {
                    srcOid = null;
                } else {
                    srcOid = targetOid;
                }
            } else if (targetOid != null) {
                srcOid = targetOid;
            } else if (srcOid != null) {
                paip = srcOid;
                srcOid = null;
            }
            if (srcOid != null) {
                // need to add $set
                return srcOid;
            }
        } else {
            // nothing since save will be done just after
        }
        return null;
    }
    /**
     * Update the link saip
     *
     * @param vtReloaded
     * @return the update part if needed
     */
    protected final String updateSaip(final VitamType vtReloaded) {
        if (vtReloaded != null) {
            String srcOid = vtReloaded.saip;
            final String targetOid = saip;
            if (srcOid != null && targetOid != null) {
                if (targetOid.equals(srcOid)) {
                    srcOid = null;
                } else {
                    srcOid = targetOid;
                }
            } else if (targetOid != null) {
                srcOid = targetOid;
            } else if (srcOid != null) {
                saip = srcOid;
                srcOid = null;
            }
            if (srcOid != null) {
                // need to add $set
                return srcOid;
            }
        } else {
            // nothing since save will be done just after
        }
        return null;
    }

    /**
     * Update the links
     *
     * @param vtReloaded
     * @return the update part
     */
    protected final Set<String> updateDaipsLinks(final VitamType vtReloaded) {
        if (vtReloaded != null) {
            final Set<String> srcList = vtReloaded.daips;
            final Set<String> targetList = daips;
            if (srcList != null && targetList != null) {
                targetList.removeAll(srcList);
            } else if (targetList != null) {
                // srcList empty
            } else {
                // targetList empty
                daips = srcList;
            }
            if (targetList != null && !targetList.isEmpty()) {
                // need to add $addToSet
                return targetList;
            }
        } else {
            // nothing since save will be done just after, except checking array exists
            if (daips == null) {
                daips = new HashSet<String>();
            }
        }
        return null;
    }
    /**
     * Update the links
     *
     * @param vtReloaded
     * @return the update part
     */
    protected final Set<String> updateDomsLinks(final VitamType vtReloaded) {
        if (vtReloaded != null) {
            final Set<String> srcList = vtReloaded.doms;
            final Set<String> targetList = doms;
            if (srcList != null && targetList != null) {
                targetList.removeAll(srcList);
            } else if (targetList != null) {
                // srcList empty
            } else {
                // targetList empty
                doms = srcList;
            }
            if (targetList != null && !targetList.isEmpty()) {
                // need to add $addToSet
                return targetList;
            }
        } else {
            // nothing since save will be done just after, except checking array exists
            if (doms == null) {
                doms = new HashSet<String>();
            }
        }
        return null;
    }
    /**
     * Update the links
     *
     * @param vtReloaded
     * @return the update part
     */
    protected final Set<String> updateUpLinks(final VitamType vtReloaded) {
        if (vtReloaded != null) {
            final Set<String> srcList = vtReloaded.up;
            final Set<String> targetList = up;
            if (srcList != null && targetList != null) {
                targetList.removeAll(srcList);
            } else if (targetList != null) {
                // srcList empty
            } else {
                // targetList empty
                up = srcList;
            }
            if (targetList != null && !targetList.isEmpty()) {
                // need to add $addToSet
                return targetList;
            }
        } else {
            // nothing since save will be done just after, except checking array exists
            if (up == null) {
                up = new HashSet<String>();
            }
        }
        return null;
    }
    /**
     * Update the links
     *
     * @param vtReloaded
     * @return the update part
     */
    protected final Set<String> updateDuasLinks(final VitamType vtReloaded) {
        if (vtReloaded != null) {
            final Set<String> srcList = vtReloaded.dua;
            final Set<String> targetList = dua;
            if (srcList != null && targetList != null) {
                targetList.removeAll(srcList);
            } else if (targetList != null) {
                // srcList empty
            } else {
                // targetList empty
                dua = srcList;
            }
            if (targetList != null && !targetList.isEmpty()) {
                // need to add $addToSet
                return targetList;
            }
        } else {
            // nothing since save will be done just after, except checking array exists
            if (dua == null) {
                dua = new HashSet<String>();
            }
        }
        return null;
    }
    /**
     * Update the links
     *
     * @param vtReloaded
     * @return the update part
     */
    protected final Set<String> updateDdsLinks(final VitamType vtReloaded) {
        if (vtReloaded != null) {
            final Set<String> srcList = vtReloaded.dds;
            final Set<String> targetList = dds;
            if (srcList != null && targetList != null) {
                targetList.removeAll(srcList);
            } else if (targetList != null) {
                // srcList empty
            } else {
                // targetList empty
                dds = srcList;
            }
            if (targetList != null && !targetList.isEmpty()) {
                // need to add $addToSet
                return targetList;
            }
        } else {
            // nothing since save will be done just after, except checking array exists
            if (dds == null) {
                dds = new HashSet<String>();
            }
        }
        return null;
    }

    /**
     * Update links (not saved to database but to file)
     *
     */
    protected final void updateDaipsLinksToFile() {
        if (daips == null) {
            daips = new HashSet<String>();
        }
    }

    /**
     * Add an asymmetric relation (1-n) between Obj1 and Obj2
     *
     * @param obj1
     * @param obj2
     * @return a {@link DBObject} for update
     */
    protected final static String addUpPaipSymmetricLink(final VitamType obj1, final VitamType obj2) {
        addPaipAsymmetricLink(obj1, obj2);
        return addUpAsymmetricLinkset(obj2, obj1, true);
    }

    /**
     * Add a symmetric relation (n-n) between Obj1 and Obj2
     *
     * @param obj1
     * @param obj2
     * @return a {@link DBObject} for update
     */
    protected final static String addDaipDomSymmetricLinkset(final VitamType obj1, final VitamType obj2) {
        addDomsAsymmetricLinkset(obj1, obj2, false);
        return addDaipsAsymmetricLinkset(obj2, obj1, true);
    }

    /**
     * Add a single relation (1) from Obj1 to Obj2
     *
     * @param obj1
     * @param obj2
     */
    protected final static void addPaipAsymmetricLink(final VitamType obj1, final VitamType obj2) {
        final String refChild = obj2.id;
        obj1.paip = refChild;
    }
    /**
     * Add a single relation (1) from Obj1 to Obj2
     *
     * @param obj1
     * @param obj2
     */
    protected final static void addSaipAsymmetricLink(final VitamType obj1, final VitamType obj2) {
        final String refChild = obj2.id;
        obj1.saip = refChild;
    }

    /**
     * Add a single relation (1) from Obj1 to Obj2 in update mode
     *
     * @param obj1
     * @param obj2
     * @return a {@link DBObject} for update
     */
    protected final static String addPaipAsymmetricLinkUpdate(final VitamType obj1, final VitamType obj2) {
        final String refChild = obj2.id;
        if (obj1.saip == refChild) {
            return null;
        }
        obj1.saip = refChild;
        return refChild;
    }

    /**
     * Add a one way relation (n) from Obj1 to Obj2, with no Save
     *
     * @param obj1
     * @param obj2
     * @return true if the link is updated
     */
    protected final static boolean addDaipsAsymmetricLinksetNoSave(final VitamType obj1, final VitamType obj2) {
        Set<String> relation12 = obj1.daips;
        final String oid2 = obj2.id;
        if (relation12 == null) {
            relation12 = new HashSet<String>();
            obj1.daips = relation12;
        }
        if (relation12.contains(oid2)) {
            return false;
        }
        relation12.add(oid2);
        return true;
    }
    /**
     * Add a one way relation (n) from Obj1 to Obj2, with no Save
     *
     * @param obj1
     * @param obj2
     * @return true if the link is updated
     */
    protected final static boolean addDdsAsymmetricLinksetNoSave(final VitamType obj1, final VitamType obj2) {
        Set<String> relation12 = obj1.dds;
        final String oid2 = obj2.id;
        if (relation12 == null) {
            relation12 = new HashSet<String>();
            obj1.dds = relation12;
        }
        if (relation12.contains(oid2)) {
            return false;
        }
        relation12.add(oid2);
        return true;
    }
    /**
     * Add a one way relation (n) from Obj1 to Obj2, with no Save
     *
     * @param obj1
     * @param obj2
     * @return true if the link is updated
     */
    protected final static boolean addDomsAsymmetricLinksetNoSave(final VitamType obj1, final VitamType obj2) {
        Set<String> relation12 = obj1.doms;
        final String oid2 = obj2.id;
        if (relation12 == null) {
            relation12 = new HashSet<String>();
            obj1.doms = relation12;
        }
        if (relation12.contains(oid2)) {
            return false;
        }
        relation12.add(oid2);
        return true;
    }
    /**
     * Add a one way relation (n) from Obj1 to Obj2, with no Save
     *
     * @param obj1
     * @param obj2
     * @return true if the link is updated
     */
    protected final static boolean addDuaAsymmetricLinksetNoSave(final VitamType obj1, final VitamType obj2) {
        Set<String> relation12 = obj1.dua;
        final String oid2 = obj2.id;
        if (relation12 == null) {
            relation12 = new HashSet<String>();
            obj1.dua = relation12;
        }
        if (relation12.contains(oid2)) {
            return false;
        }
        relation12.add(oid2);
        return true;
    }
    /**
     * Add a one way relation (n) from Obj1 to Obj2, with no Save
     *
     * @param obj1
     * @param obj2
     * @return true if the link is updated
     */
    protected final static boolean addUpAsymmetricLinksetNoSave(final VitamType obj1, final VitamType obj2) {
        Set<String> relation12 = obj1.up;
        final String oid2 = obj2.id;
        if (relation12 == null) {
            relation12 = new HashSet<String>();
            obj1.up = relation12;
        }
        if (relation12.contains(oid2)) {
            return false;
        }
        relation12.add(oid2);
        return true;
    }

    /**
     * Add a one way relation (n) from Obj1 to Obj2
     *
     * @param obj1
     * @param obj2
     * @param toUpdate
     *            True if this element will be updated through $addToSet only
     * @return a {@link DBObject} for update
     */
    protected final static String addDaipsAsymmetricLinkset(final VitamType obj1, final VitamType obj2,
            final boolean toUpdate) {
        Set<String> relation12 = obj1.daips;
        final String oid2 = obj2.id;
        if (relation12 == null) {
            if (toUpdate) {
                return oid2;
            }
            relation12 = new HashSet<String>();
            obj1.daips = relation12;
        }
        if (relation12.contains(oid2)) {
            return null;
        }
        if (toUpdate) {
            return oid2;
        }
        relation12.add(oid2);
        return null;
    }

    /**
     * Add a one way relation (n) from Obj1 to Obj2
     *
     * @param obj1
     * @param obj2
     * @param toUpdate
     *            True if this element will be updated through $addToSet only
     * @return a {@link DBObject} for update
     */
    protected final static String addDdsAsymmetricLinkset(final VitamType obj1, final VitamType obj2,
            final boolean toUpdate) {
        Set<String> relation12 = obj1.dds;
        final String oid2 = obj2.id;
        if (relation12 == null) {
            if (toUpdate) {
                return oid2;
            }
            relation12 = new HashSet<String>();
            obj1.dds = relation12;
        }
        if (relation12.contains(oid2)) {
            return null;
        }
        if (toUpdate) {
            return oid2;
        }
        relation12.add(oid2);
        return null;
    }

    /**
     * Add a one way relation (n) from Obj1 to Obj2
     *
     * @param obj1
     * @param obj2
     * @param toUpdate
     *            True if this element will be updated through $addToSet only
     * @return a {@link DBObject} for update
     */
    private final static String addDomsAsymmetricLinkset(final VitamType obj1, final VitamType obj2,
            final boolean toUpdate) {
        Set<String> relation12 = obj1.doms;
        final String oid2 = obj2.id;
        if (relation12 == null) {
            if (toUpdate) {
                return oid2;
            }
            relation12 = new HashSet<String>();
            obj1.doms = relation12;
        }
        if (relation12.contains(oid2)) {
            return null;
        }
        if (toUpdate) {
            return oid2;
        }
        relation12.add(oid2);
        return null;
    }

    /**
     * Add a one way relation (n) from Obj1 to Obj2
     *
     * @param obj1
     * @param obj2
     * @param toUpdate
     *            True if this element will be updated through $addToSet only
     * @return a {@link DBObject} for update
     */
    protected final static String addDuaAsymmetricLinkset(final VitamType obj1, final VitamType obj2,
            final boolean toUpdate) {
        Set<String> relation12 = obj1.dua;
        final String oid2 = obj2.id;
        if (relation12 == null) {
            if (toUpdate) {
                return oid2;
            }
            relation12 = new HashSet<String>();
            obj1.dua = relation12;
        }
        if (relation12.contains(oid2)) {
            return null;
        }
        if (toUpdate) {
            return oid2;
        }
        relation12.add(oid2);
        return null;
    }

    /**
     * Add a one way relation (n) from Obj1 to Obj2
     *
     * @param obj1
     * @param obj2
     * @param toUpdate
     *            True if this element will be updated through $addToSet only
     * @return a {@link DBObject} for update
     */
    private final static String addUpAsymmetricLinkset(final VitamType obj1, final VitamType obj2,
            final boolean toUpdate) {
        Set<String> relation12 = obj1.up;
        final String oid2 = obj2.id;
        if (relation12 == null) {
            if (toUpdate) {
                return oid2;
            }
            relation12 = new HashSet<String>();
            obj1.up = relation12;
        }
        if (relation12.contains(oid2)) {
            return null;
        }
        if (toUpdate) {
            return oid2;
        }
        relation12.add(oid2);
        return null;
    }

}
