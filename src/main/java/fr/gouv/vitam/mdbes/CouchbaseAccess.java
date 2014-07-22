/**
 * This file is part of Vitam Project.
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All Vitam Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * 
 * Vitam is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with Vitam . If not, see
 * <http://www.gnu.org/licenses/>.
 */

package fr.gouv.vitam.mdbes;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.FailureMode;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import com.fasterxml.jackson.databind.JsonNode;

import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.exception.InvalidParseOperationException;
import fr.gouv.vitam.query.json.JsonHandler;
import fr.gouv.vitam.utils.FileUtil;
import fr.gouv.vitam.utils.GlobalDatas;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * @author "Frederic Bregier"
 *
 */
public class CouchbaseAccess {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(CouchbaseAccess.class);
    
    protected List<URI> hosts;
    protected String bucket = "VitamRequests";
    protected String password = "";
    protected CouchbaseClient client;
    protected View result_nb = null;
    protected MessageDigest md;
    /**
     * Connect to the Couchbase database
     * @param hosts
     * @param bucket
     * @param password
     * @throws InvalidCreateOperationException
     */
    public CouchbaseAccess(List<URI> hosts, String bucket, String password) throws InvalidCreateOperationException {
        this.hosts = hosts;
        this.bucket = bucket;
        this.password = password;
        CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        cfb.setFailureMode(FailureMode.Retry);
        CouchbaseConnectionFactory cf;
        try {
            cf = cfb.buildCouchbaseConnection(hosts, bucket, password);
            client = new CouchbaseClient(cf);
        } catch (IOException e1) {
            LOGGER.error(e1);
            throw new InvalidCreateOperationException(e1);
        }
        if (! GlobalDatas.USEMEMCACHED) {
            result_nb = client.getView("results", "result_nb");
        }
        try {
            md = MessageDigest.getInstance("SHA-512");
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error(e);
        }
    }
    /**
     * Close the underlying connection
     */
    public void close() {
        client.shutdown(10, TimeUnit.SECONDS);
    }
    /**
     * 
     * @return the number of ResultCached documents
     */
    public final long getCount() {
        if (result_nb == null) {
            Map<SocketAddress,Map<String,String>> stats = client.getStats();
            Map<String,String> map = stats.get(stats.keySet().iterator().next());
            return Long.parseLong(map.get("curr_items"));
        }
        Query query = new Query();
        query.setIncludeDocs(false);
        query.setLimit(1);
        query.setStale(Stale.FALSE);
        ViewResponse response = client.query(result_nb, query);
        if (response.size() == 0) {
            return 0;
        }
        ViewRow row = response.removeLastElement();
        return Long.parseLong(row.getValue());
    }
    /**
     * 
     * @param tohash
     * @return the corresponding digest as a "false" String
     */
    public final String createDigest(final String tohash) {
        synchronized (md) {
            md.update(tohash.getBytes(FileUtil.UTF8));
            return new String(md.digest());
        }
    }

    /**
     * 
     * @param id
     * @return True if this item exists
     */
    public final boolean exists(final String id) {
        final String nid = createDigest(id);
        return (client.get(nid) != null);
    }
    /**
     * 
     * @param id
     * @return the Json object corresponding to the id from the database or null if an error occurs
     * or if not found
     */
    public final JsonNode getFromId(final String id) {
        final String nid = createDigest(id);
        String value = (String) client.get(nid);
        if (value == null)
            return null;
        try {
            return JsonHandler.getFromString(value);
        } catch (InvalidParseOperationException e) {
            LOGGER.error(e);
            return null;
        }
    }
    /**
     * Set the JsonNode as Id in database
     * @param id
     * @param node
     * @param ttl time to live in seconds
     * @return True if OK
     */
    public final boolean setToId(final String id, final JsonNode node, final int ttl) {
        final String nid = createDigest(id);
        String value = node.toString();
        try {
            return client.set(nid, ttl, value).get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error(e);
            return false;
        }
    }
    /**
     * Update the time to live only
     * @param id
     * @param ttl
     * @return True of OK
     */
    public final boolean updateTtl(final String id, final int ttl) {
        final String nid = createDigest(id);
        try {
            return client.touch(nid, ttl).get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error(e);
            return false;
        }
    }
}
