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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.fasterxml.jackson.databind.JsonNode;

import fr.gouv.vitam.utils.exception.InvalidParseOperationException;
import fr.gouv.vitam.utils.json.JsonHandler;
import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * @author "Frederic Bregier"
 *
 */
public class RedisAccess {
    private static final VitamLogger LOGGER = VitamLoggerFactory.getInstance(RedisAccess.class);
    
    protected static String host = null;
    protected static JedisPool pool;
    protected Jedis jedis;
    //protected MessageDigest md;
    /**
     * Connect to the Redis database
     * @param host
     * @param poolSize
     */
    public RedisAccess(String host, int poolSize) {
        synchronized (LOGGER) {
            if (RedisAccess.host == null) {
                RedisAccess.host = host;
                JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
                jedisPoolConfig.setMaxTotal(poolSize);
                pool = new JedisPool(jedisPoolConfig, host);
            }
        }
        try {
            jedis = pool.getResource();
        } catch (Exception e) {
            LOGGER.error("Could not connect to jedis", e);
            jedis = null;
            finalClose();
        }
        /*try {
            md = MessageDigest.getInstance("SHA-512");
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error(e);
        }*/
    }
    /**
     * Close the underlying connection
     */
    public void close() {
        if (jedis != null) {
            jedis.close();
            jedis = null;
        }
    }
    /**
     * Finalize Redis support
     */
    public void finalClose() {
        if (jedis != null) {
            jedis.close();
            jedis = null;
        }
        if (RedisAccess.host != null) {
            pool.destroy();
            RedisAccess.host = null;
        }
    }
    /**
     * 
     * @return the number of ResultCached documents
     */
    public final long getCount() {
        if (jedis == null) {
            return 0;
        }
        //LOGGER.warn(jedis.info("Keyspace"));
        return jedis.dbSize();
    }
    /**
     * 
     * @param tohash
     * @return the corresponding digest as a "false" String
     */
    public final String createDigest(final String tohash) {
        return tohash;
        /*synchronized (md) {
            md.update(tohash.getBytes(FileUtil.UTF8));
            return new String(md.digest());
        }*/
    }

    /**
     * 
     * @param id
     * @return True if this item exists
     */
    public final boolean exists(final String id) {
        if (jedis == null) {
            return false;
        }
        final String nid = createDigest(id);
        return jedis.exists(nid);
    }
    /**
     * 
     * @param id
     * @return the Json object corresponding to the id from the database or null if an error occurs
     * or if not found
     */
    public final JsonNode getFromId(final String id) {
        if (jedis == null) {
            return null;
        }
        final String nid = createDigest(id);
        String value = jedis.get(nid);
        if (value == null) {
            return null;
        }
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
        if (jedis == null) {
            return true;
        }
        final String nid = createDigest(id);
        String value = node.toString();
        String status = jedis.set(nid, value);
        return (status != null && status.equalsIgnoreCase("ok"));
    }
    /**
     * Update the time to live only
     * @param id
     * @param ttl
     * @return True of OK
     */
    public final boolean updateTtl(final String id, final int ttl) {
        return true;
        //final String nid = createDigest(id);
        //return (jedis.expire(nid, ttl) == 1);
    }
}
