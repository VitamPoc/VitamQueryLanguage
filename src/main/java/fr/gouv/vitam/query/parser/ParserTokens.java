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
package fr.gouv.vitam.query.parser;

/**
 * @author "Frederic Bregier"
 *
 */
public class ParserTokens {
    /**
     * 
     * For a Query : { $query : select, $filter : filter, $projection : projection } or [ select, filter, projection ]</br>
     * For an Update : { $query : select, $filter : multi, $action : action } or [ select, multi, action ]</br>
     * For an Insert : { $query : select, $filter : multi, $data : data } or [ select, multi, data ]</br>
     * For a Delete : { $query : select, $filter : multi } or [ select, multi ]</br>
     *
     * Select is in a subtree, by default next level (1), except if $depth is set with 
     * a value (exact depth) or $relativedepth with a relative value (+ or -, meaning leaves or parents, 0 for no limit in leaves depth)
     * Only one of $depth and $relativedepth might be set. If both are set, only $depth will be kept.
     * { expression, $depth : exactdepth, $relativedepth : /- depth }
     */
    public static enum GLOBAL {
        query, filter, projection, action, data;
        
        public final String exactToken() {
            return "$"+this.name();
        }
    }
    public static enum REQUEST { 
        /**
         * All expressions are grouped by an AND operator (all shall be true)
         * $and : [ expression1, expression2, ... ]
         */
        and, 
        /**
         * All expressions are grouped by an NOR operator (only one shall be true)
         * $nor : [ expression1, expression2, ... ]
         */
        nor, 
        /**
         * All expressions are grouped by an OR operator (at least one shall be true)
         * $or : [ expression1, expression2, ... ]
         */
        or, 
        /**
         * All expressions are grouped by an NOT operator (negation of implicit AND of all expressions)
         * $not : [ expression1, expression2, ... ]
         */
        not, 
        /**
         * Field named 'name' shall exist
         * $exists : name
         */
        exists, 
        /**
         * Field named 'name' shall not exist (faster than $not : [ $exists : name ] )
         * $missing : name
         */
        missing, 
        /**
         * Field named 'name' shall be empty or set to null
         * $isNull : name
         */
        isNull,
        /**
         * field named 'name' contains at least one of the values 'value1', 'value2', ...
         * $in : { name : [ value1, value2, ... ] }
         */
        in, 
        /**
         * field named 'name' does not contain any of the values 'value1', 'value2', ...
         * $nin : { name : [ value1, value2, ... ] }
         */
        nin, 
        /**
         * Size of an array named 'name' equals to specified length
         * $size : { name : length }
         */
        size, 
        /**
         * Comparison operator
         * $gt : { name : value }
         */
        gt, 
        /**
         * Comparison operator
         * $lt : { name : value }
         */
        lt, 
        /**
         * Comparison operator
         * $gte : { name : value }
         */
        gte, 
        /**
         * Comparison operator
         * $lte : { name : value }
         */
        lte, 
        /**
         * Comparison operator
         * $ne : { name : value }
         */
        ne, 
        /**
         * Comparison operator
         * $eq : { name : value }
         */
        eq,
        /**
         * Optimization of comparison operator in a range
         * $range : { name : { $gte : value, $lte : value } }
         */
        range,
        /**
         * type might be Point (simple lng, lta), Box, Polygon
         * $geometry : { $type : "type", $coordinates : [ [ lng1, lta1 ], [ lng2, lta2 ], ... ] }
         */
        geometry, 
        /**
         * $box : [ [ lng1, lta1 ], [ lng2, lta2 ] ]
         */
        box,
        /**
         * $polygon : [ [ lng1, lta1 ], [ lng2, lta2 ], ... ]
         */
        polygon,
        /**
         * $center : [ [ lng1, lta1 ], radius ]
         */
        center,
        /**
         * Selects geometries within a bounding geometry
         * $geoWithin : { name : { geometry|box|polygon|center } }
         */
        geoWithin, 
        /**
         * Selects geometries that intersect with a geometry
         * $geoIntersects : { name : { geometry|box|polygon|center } }
         */
        geoIntersects, 
        /**
         * Selects geometries in proximity to a point
         * $near : { name : { geometry_point|[ lng1, lta1], $maxDistance : distance } }
         */
        near, 
        /**
         * Selects where field named 'name' matches some words
         * $match : { name : words, $max_expansions : n }
         */
        match, 
        /**
         * Selects where field named 'name' matches a phrase (somewhere)
         * $match_phrase : { name : phrase, $max_expansions : n }
         */
        match_phrase, 
        /**
         * Selects where field named 'name' matches a phrase as a prefix of the field
         * $match_phrase_prefix : { name : phrase, $max_expansions : n }
         */
        match_phrase_prefix,
        /**
         * To not be used externally but in replacement of match_phrase_prefix if parameter not analyzed 
         */
        prefix,
        /**
         * Selects where fields named 'name' are like the one provided, introducing some "fuzzy", which tends to be slower than mlt
         * $flt : { $fields : [ name1, name2 ], $like : like_text }
         */
        flt, 
        /**
         * Selects where fields named 'name' are like the one provided
         * $mlt : { $fields : [ name1, name2 ], $like : like_text }
         */
        mlt, 
        /**
         * Selects where field named 'name' contains something relevant to the search parameter.
         * This search parameter can contain wildcards (* ?), specifications:</br>
         *  (x y) meaning x or y</br>
         *  "x y" meaning exactly sub phrase "x y"</br>
         *  +x -y meaning x must be present, y must be absent</br>
         *  $search : { name : searchParameter }
         */
        search,
        /**
         * Selects where field named 'name' contains a value valid with the corresponding regular expression.
         * $regex : { name : regex }
         */
        regex,
        /**
         * Selects where field named 'name' contains exactly this term (lowercase only, no blank). 
         * Useful in simple value field to find one specific item, or for multiple values field (blank separated string list) to find one item containing this term. 
         * $term : { name : term, name : term }
         */
        term,
        /**
         * Selects a node by its exact path (succession of ids)
         * $path : [ id1, id2, ... ]
         */
        path;
        
        public final String exactToken() {
            return "$"+this.name();
        }
    }
    
    public static enum REQUESTFILTER {
        /**
         * Limit the elements returned to the nth first elements
         * $limit : n
         */
        limit,
        /**
         * According to an orderby, start to return the elements from rank start
         * $offset : start 
         */
        offset, 
        /**
         * Specify an orderby to respect in the return of the elements according to
         * one field named 'name' and an orderby ascendant (+1) or descendant (-1)
         * $orderby : [ { key : +/-1 } ]
         */
        orderby, 
        /**
         * Allows to specify some hints to the query server: cache/nocache
         * $hint : [ cache/nocache, ... ]
         */
        hint;
        
        public final String exactToken() {
            return "$"+this.name();
        }
    }
    
    public static enum PROJECTION {
        /**
         * Specify the fields to return
         * $fields : {name1 : 0/1, name2 : 0/1, ...}
         */
        fields,
        /**
         * UsageContract reference that will be used to select the binary object version to return
         * $usage : contractId
         */
        usage;
        
        public final String exactToken() {
            return "$"+this.name();
        }
    }

    /**
     * Arguments to REQUEST commands
     *
     */
    public static enum REQUESTARGS { 
        type, coordinates, maxDistance, like, fields, max_expansions, depth, relativedepth;
        
        public final String exactToken() {
            return "$"+this.name();
        }
    }
    
    public static enum RANGEARGS {
        /**
         * Comparison operator
         * $gt : value
         */
        gt, 
        /**
         * Comparison operator
         * $lt : value 
         */
        lt, 
        /**
         * Comparison operator
         * $gte : value 
         */
        gte, 
        /**
         * Comparison operator
         * $lte : value 
         */
        lte;
        
        public final String exactToken() {
            return "$"+this.name();
        }
    }
    
    /**
     * specific fields: nbleaves, dua, ...
     * $fields : [ @nbleaves:1, @dua:1, @all:1... ]
     * @all:1 means all, while @all:0 means none
     */
    public static enum PROJECTIONARGS {
        nbleaves, dua, all;
        
        public final String exactToken() {
            return "@"+this.name();
        }
    }

    /**
     * Specific values for Filter arguments
     *
     */
    public static enum FILTERARGS {
        cache, nocache;
        
        public final String exactToken() {
            return this.name();
        }
    }
    
    public static enum UPDATE {
        /**
         * $set : { name : value, name : value, ... }
         */
        set,
        /**
         * $unset : [ name, name, ... ]
         */
        unset,
        /**
         * increment one field named 'name' with default 1 or value 
         * $inc : { name : value }
         */
        inc,
        /**
         * rename one field named 'name' to 'newname'
         * $rename : { name : newname }
         */
        rename,
        /**
         * Add one element at the end of a list value, or each element of a list if $each parameter is used
         * $push : { name : value }
         * $push : { name : { $each : [ value, value, ... ] } }
         */
        push,
        /**
         * Remove n element from a list from the end (n) or the beginning (-n)
         * $pull : { name : n/-n }
         */
        pull,
        /**
         * Add one element (or each element of a list) if not already in the list
         * $add : { name : value }
         * $add : { name : { $each : [ value, value, ... ] } }
         */
        add,
        /**
         * Remove one specific element from a list or each element of a list if $each parameter is used
         * $pop : { name : value }
         * $pop : { name : { $each : [ value, value, ... ] } }
         */
        pop;
        
        public final String exactToken() {
            return "$"+this.name();
        }
    }
    public static enum UPDATEARGS {
        each;
        public final String exactToken() {
            return "$"+this.name();
        }
    }
    public static enum ACTIONFILTER {
        /**
         * True to allow multiple update if multiple elements are found through the REQUEST, 
         * else False will return an error if multiple elements are found.
         * $mult : true/false
         */
        mult;
        
        public final String exactToken() {
            return "$"+this.name();
        }
    }
}
