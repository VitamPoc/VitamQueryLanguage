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
package fr.gouv.vitam.query.json;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.exception.InvalidParseOperationException;

/**
 * JSON handler using Json format
 *
 * @author "Frederic Bregier"
 *
 */
public final class JsonHandler {

    /**
     * Default JsonFactory
     */
    private final static JsonFactory JSONFACTORY = new JsonFactory();
    /**
     * Default ObjectMapper
     */
    private final static ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = new ObjectMapper(JSONFACTORY);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        OBJECT_MAPPER.configure(JsonGenerator.Feature.ESCAPE_NON_ASCII, true);
    }

    private JsonHandler() {
    }

    /**
     *
     * @return an empty ObjectNode
     */
    public static final ObjectNode createObjectNode() {
        return OBJECT_MAPPER.createObjectNode();
    }

    /**
     * @return an empty ArrayNode
     */
    public static final ArrayNode createArrayNode() {
        return OBJECT_MAPPER.createArrayNode();
    }

    /**
     *
     * @param value
     * @return the jsonNode (ObjectNode or ArrayNode)
     * @throws InvalidParseOperationException
     */
    public static final JsonNode getFromString(final String value) throws InvalidParseOperationException {
        try {
            return OBJECT_MAPPER.readTree(value);
        } catch (final JsonProcessingException e) {
            throw new InvalidParseOperationException(e);
        } catch (final IOException e) {
            throw new InvalidParseOperationException(e);
        }
    }

    /**
     *
     * @param value
     * @return the jsonNode (ObjectNode or ArrayNode)
     * @throws InvalidParseOperationException
     */
    public static final JsonNode getFromBytes(final byte[] value) throws InvalidParseOperationException {
        try {
            return OBJECT_MAPPER.readTree(value);
        } catch (final JsonProcessingException e) {
            throw new InvalidParseOperationException(e);
        } catch (final IOException e) {
            throw new InvalidParseOperationException(e);
        }
    }

    /**
     *
     * @param object
     * @return the Json representation of the object
     * @throws InvalidParseOperationException
     */
    public static final String writeAsString(final Object object) throws InvalidParseOperationException {
        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        } catch (final JsonProcessingException e) {
            throw new InvalidParseOperationException(e);
        }
    }

    /**
     * node should have only one property
     *
     * @param nodeName
     * @param node
     * @return the couple property name and property value
     * @throws InvalidParseOperationException
     */
    public static final Entry<String, JsonNode> checkUnicity(final String nodeName, final JsonNode node)
            throws InvalidParseOperationException {
        if (node == null || node.isMissingNode()) {
            throw new InvalidParseOperationException("The current Node is missing(empty): " + nodeName + ":" + node);
        }
        if (node.isValueNode()) {
            // not allowed
            throw new InvalidParseOperationException("The current Node is a simple value and should not: " + nodeName + ":"
                    + node);
        }
        final int size = node.size();
        if (size > 1) {
            throw new InvalidParseOperationException("More than one element in current Node: " + nodeName + ":" + node);
        }
        if (size == 0) {
            throw new InvalidParseOperationException("Not enough element (0) in current Node: " + nodeName + ":" + node);
        }
        final Iterator<Entry<String, JsonNode>> iterator = node.fields();
        return iterator.next();
    }

    /**
     * node should have only one property ; simple value is allowed
     *
     * @param nodeName
     * @param node
     * @return the couple property name and property value
     * @throws InvalidParseOperationException
     */
    public static final Entry<String, JsonNode> checkLaxUnicity(final String nodeName, final JsonNode node)
            throws InvalidParseOperationException {
        if (node == null || node.isMissingNode()) {
            throw new InvalidParseOperationException("The current Node is missing(empty): " + nodeName + ":" + node);
        }
        if (node.isValueNode()) {
            // already one node
            return new Entry<String, JsonNode>() {
                @Override
                public JsonNode setValue(final JsonNode value) {
                    throw new IllegalArgumentException("Cannot set Value");
                }

                @Override
                public JsonNode getValue() {
                    return node;
                }

                @Override
                public String getKey() {
                    return null;
                }
            };
        }
        final int size = node.size();
        if (size > 1) {
            throw new InvalidParseOperationException("More than one element in current Node: " + nodeName + ":" + node);
        }
        if (size == 0) {
            throw new InvalidParseOperationException("Not enough element (0) in current Node: " + nodeName + ":" + node);
        }
        final Iterator<Entry<String, JsonNode>> iterator = node.fields();
        return iterator.next();
    }

    /**
     *
     * @param value
     * @return the corresponding HashMap
     * @throws InvalidParseOperationException
     */
    public static final Map<String, Object> getMapFromString(final String value) throws InvalidParseOperationException {
        if (value != null && !value.isEmpty()) {
            Map<String, Object> info = null;
            try {
                info = OBJECT_MAPPER.readValue(value, new TypeReference<Map<String, Object>>() {
                });
            } catch (final JsonParseException e) {
                throw new InvalidParseOperationException(e);
            } catch (final JsonMappingException e) {
                throw new InvalidParseOperationException(e);
            } catch (final IOException e) {
                throw new InvalidParseOperationException(e);
            }
            if (info == null) {
                info = new HashMap<String, Object>();
            }
            return info;
        } else {
            return new HashMap<String, Object>();
        }
    }

}
