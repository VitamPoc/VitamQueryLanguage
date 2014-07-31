/**
 * 
 */
function varietyTypeOf(thing) {
  if (typeof thing === 'undefined') { throw 'varietyTypeOf() requires an argument'; }
  if (typeof thing !== 'object') { return (typeof thing)[0].toUpperCase() + (typeof thing).slice(1); }
  else { if (thing && thing.constructor === Array) { return 'Array'; }
    else if (thing === null) { return 'null'; }
    else if (thing instanceof Date) { return 'Date'; }
    else if (thing instanceof ObjectId) { return 'ObjectId'; }
    else if (thing instanceof BinData) { var binDataTypes = {};
      binDataTypes[0x00] = 'generic'; binDataTypes[0x01] = 'function'; binDataTypes[0x02] = 'old';
      binDataTypes[0x03] = 'UUID'; binDataTypes[0x05] = 'MD5'; binDataTypes[0x80] = 'user';
      return 'BinData-' + binDataTypes[thing.subtype()]; }
    else { return 'Object'; } } }

function serializeDoc(doc, maxDepth){
    var result = {};
    function isHash(v) { var isArray = Array.isArray(v);
      var isObject = typeof v === 'object';
      var specialObject = v instanceof Date || v instanceof ObjectId || v instanceof BinData;
      return !specialObject && !isArray && isObject; }

    function serialize(document, parentKey, maxDepth){
        for(var key in document){
            if(!(document.hasOwnProperty(key))) { continue; }
            var value = document[key];
            result[parentKey+key] = value;
            if(isHash(value) && (maxDepth > 0)) { serialize(value, parentKey+key+'.',maxDepth-1); } } }
    serialize(doc, '', maxDepth);
    return result; }

function() {
	flattened = serializeDoc(this, 5);
    for (var key in flattened) {
    	var value = flattened[key]; var valueType = varietyTypeOf(value);
        emit(key, { types: { valueType : true }, occurences: 1}); } }



function() {
	function varietyTypeOf(thing) {
		if (typeof thing === 'undefined') { throw 'varietyTypeOf() requires an argument'; }
		if (typeof thing !== 'object') { return (typeof thing)[0].toUpperCase() + (typeof thing).slice(1); }
		else { if (thing && thing.constructor === Array) { return 'Array'; }
		else if (thing === null) { return 'null'; }
		else if (thing instanceof Date) { return 'Date'; }
		else if (thing instanceof ObjectId) { return 'ObjectId'; }
		else if (thing instanceof BinData) { var binDataTypes = {};
		binDataTypes[0x00] = 'generic'; binDataTypes[0x01] = 'function'; binDataTypes[0x02] = 'old';
		binDataTypes[0x03] = 'UUID'; binDataTypes[0x05] = 'MD5'; binDataTypes[0x80] = 'user';
		return 'BinData-' + binDataTypes[thing.subtype()]; }
		else { return 'Object'; } 
		}
	}
	function serializeDoc(doc, maxDepth){
		var result = {};
		function isHash(v) { var isArray = Array.isArray(v);
		var isObject = typeof v === 'object';
		var specialObject = v instanceof Date || v instanceof ObjectId || v instanceof BinData;
		return !specialObject && !isArray && isObject; }

		function serialize(document, parentKey, maxDepth){
			for(var key in document){
				if(!(document.hasOwnProperty(key))) { continue; }
				var value = document[key];
				result[parentKey+key] = value;
				if(isHash(value) && (maxDepth > 0)) { serialize(value, parentKey+key+'.',maxDepth-1); } } }
		serialize(doc, '', maxDepth);
		return result; 
	}
	function Set() {
		this.content = {};
	}
	Set.prototype.add = function(o) { this.content[o] = true; }
	Set.prototype.asArray = function() { var res = []; for (var val in this.content) res.push(val); return res; }
	
	flattened = serializeDoc(this, 5);
	for (var key in flattened) {
		var value = flattened[key]; var valueType = varietyTypeOf(value);
		var myset = new Set();
		myset.add(valueType);
		var finalvalue = { types : myset.asArray(), occurences : 1};
		emit(key, finalvalue); 
	} 
}

function(key,values) {
	function Set() {
		this.content = {};
	}
	Set.prototype.add = function(o) { this.content[o] = true; }
	Set.prototype.asArray = function() { var res = []; for (var val in this.content) res.push(val); return res; }
	var typeset = new Set();
	var occur = 0;
	for (var idx = 0; idx < values.length; idx++) {
		occur += values[idx].occurences;
		typeset.add(values[idx].types); 
	}
	return { types : typeset.asArray(), occurences : occur };
}

function() {
	function serializeDoc(doc, maxDepth){
	    var result = {};
	    function isHash(v) { var isArray = Array.isArray(v);
	      var isObject = typeof v === 'object';
	      var specialObject = v instanceof Date || v instanceof ObjectId || v instanceof BinData;
	      return !specialObject && !isArray && isObject; }

	    function serialize(document, parentKey, maxDepth){
	        for(var key in document){
	            if(!(document.hasOwnProperty(key))) { continue; }
	            var value = document[key];
	            result[parentKey+key] = value;
	            if(isHash(value) && (maxDepth > 0)) { serialize(value, parentKey+key+'.',maxDepth-1); } } }
	    serialize(doc, '', maxDepth);
	    return result; }
	flattened = serializeDoc(this, 5);
    for (var key in flattened) {
        emit(key, 1); } }

function(key,values) {
	var reduced = { occurences : 0 };
	for (var idx = 0; idx < values.length; idx++) {
		reduced.occurences += values[idx].occurences;
	}
	return reduced; }

// TEST in MONGO shell

var varietyTypeOf = function varietyTypeOf(thing) {
	if (typeof thing === 'undefined') { throw 'varietyTypeOf() requires an argument'; }
	if (typeof thing !== 'object') { return (typeof thing)[0].toUpperCase() + (typeof thing).slice(1); }
	else { if (thing && thing.constructor === Array) { return 'Array'; }
	else if (thing === null) { return 'null'; }
	else if (thing instanceof Date) { return 'Date'; }
	else if (thing instanceof ObjectId) { return 'ObjectId'; }
	else if (thing instanceof BinData) { var binDataTypes = {};
	binDataTypes[0x00] = 'generic'; binDataTypes[0x01] = 'function'; binDataTypes[0x02] = 'old';
	binDataTypes[0x03] = 'UUID'; binDataTypes[0x05] = 'MD5'; binDataTypes[0x80] = 'user';
	return 'BinData-' + binDataTypes[thing.subtype()]; }
	else { return 'Object'; } 
	}
}

var serializeDoc = function serializeDoc(doc, maxDepth){
	var result = {};
	function isHash(v) { var isArray = Array.isArray(v);
	var isObject = typeof v === 'object';
	var specialObject = v instanceof Date || v instanceof ObjectId || v instanceof BinData;
	return !specialObject && !isArray && isObject; }

	function serialize(document, parentKey, maxDepth){
		for(var key in document){
			if(!(document.hasOwnProperty(key))) { continue; }
			var value = document[key];
			result[parentKey+key] = value;
			if(isHash(value) && (maxDepth > 0)) { serialize(value, parentKey+key+'.',maxDepth-1); } } }
	serialize(doc, '', maxDepth);
	return result; 
}

var map = function() {
	flattened = serializeDoc(this, 5);
	for (var key in flattened) {
		var value = flattened[key]; 
		var valueType = varietyTypeOf(value);
		var finalvalue = { types : [ valueType ], occurences : 1};
		emit(key, finalvalue); 
	} 
}
var emit = function(key, value) { 
	print("Emit Key: "+key+" value: "+tojson(value)); 
}

use VitamLinks

var val = db.DAip.findOne({});
map.apply(val);


var Set = function Set() {
	this.content = {};
	Set.prototype.add = function(o) { this.content[o] = true; }
	Set.prototype.asArray = function() { var res = []; for (var val in this.content) res.push(val); return res; }
}

var red = function(key,values) {
	var typeset = new Set();
	var occur = 0;
	for (var idx = 0; idx < values.length; idx++) {
		occur += values[idx].occurences;
		typeset.add(values[idx].types); 
	}
	return { types : typeset.asArray(), occurences : occur };
}
var myTests = [ { "types" :  ["String"], "occurences" : 1 }, { "types" : [ "Number" ], "occurences" : 1 }, { "types" : [ "String" ], "occurences" : 1 }, { "types" : [ "Object" ], "occurences" : 1 }, { "types" : [ "Number" ], "occurences" : 1 } ]
red('_id', myTests);



// system.js
use VitamLinks
//identification du type
db.system.js.save({
	_id: 'varietyTypeOf',
	value : function varietyTypeOf(thing) {
		if (typeof thing === 'undefined') { throw 'varietyTypeOf() requires an argument'; }
		if (typeof thing !== 'object') { return (typeof thing)[0].toUpperCase() + (typeof thing).slice(1); }
		else { if (thing && thing.constructor === Array) { return 'Array'; }
		else if (thing === null) { return 'null'; }
		else if (thing instanceof Date) { return 'Date'; }
		else if (thing instanceof ObjectId) { return 'ObjectId'; }
		else if (thing instanceof BinData) { var binDataTypes = {};
		binDataTypes[0x00] = 'generic'; binDataTypes[0x01] = 'function'; binDataTypes[0x02] = 'old';
		binDataTypes[0x03] = 'UUID'; binDataTypes[0x05] = 'MD5'; binDataTypes[0x80] = 'user';
		return 'BinData-' + binDataTypes[thing.subtype()]; }
		else { return 'Object'; } 
		}
	}
})
// serialisation des variables et compteurs
db.system.js.save({
	_id: 'serializeDoc',
	value : function serializeDoc(doc, maxDepth){
		var result = {};
		function isHash(v) { var isArray = Array.isArray(v);
		var isObject = typeof v === 'object';
		var specialObject = v instanceof Date || v instanceof ObjectId || v instanceof BinData;
		return !specialObject && !isArray && isObject; }

		function serialize(document, parentKey, maxDepth){
			for(var key in document){
				if(!(document.hasOwnProperty(key))) { continue; }
				var value = document[key];
				result[parentKey+key] = value;
				if(isHash(value) && (maxDepth > 0)) { serialize(value, parentKey+key+'.',maxDepth-1); } } }
		serialize(doc, '', maxDepth);
		return result; 
	}
})
// implémentation simpliste d'un "set" avec add() et asArray()
db.system.js.save({
	_id: 'Set',
	value : function Set() {
		this.content = {};
		Set.prototype.add = function(o) { this.content[o] = true; }
        Set.prototype.remove = function(o) { delete this.content[o]; }
        Set.prototype.contains = function(o) { return (o in this.content); }
		Set.prototype.asArray = function() { var res = []; for (var val in this.content) res.push(val); return res; }
	}
})

function() {
	flattened = serializeDoc(this, 5);
	for (var key in flattened) {
		var value = flattened[key]; 
		var valueType = varietyTypeOf(value);
		var finalvalue = { types : [ valueType ], occurences : 1};
		emit(key, finalvalue); 
	} 
}

function(key,values) {
	var typeset = new Set();
	var occur = 0;
	for (var idx = 0; idx < values.length; idx++) {
		occur += values[idx].occurences;
		typeset.add(values[idx].types); 
	}
	return { types : typeset.asArray(), occurences : occur };
}

// serialisation des variables et compteurs
db.system.js.save({
    _id: 'serializeDocFiltered',
    value : function serializeDocFiltered(doc, maxDepth, filter){
        var result = {};
        function isHash(v) { var isArray = Array.isArray(v);
        var isObject = typeof v === 'object';
        var specialObject = v instanceof Date || v instanceof ObjectId || v instanceof BinData;
        return !specialObject && !isArray && isObject; }

        function serialize(document, parentKey, maxDepth){
            for(var key in document){
                                var newkey = parentKey+key;
                if(newkey.lastIndexOf(filter, 0) === 0) { continue; }
                if(!(document.hasOwnProperty(key))) { continue; }
                var value = document[key];
                result[newkey] = value;
                if(isHash(value) && (maxDepth > 0)) { serialize(value, newkey+'.',maxDepth-1); } } }
        serialize(doc, '', maxDepth);
        return result;
    }
}) 

function() {
	flattened = serializeDocFiltered(this, MAXDEPTH, FILTER);
	for (var key in flattened) {
		var value = flattened[key]; 
		var valueType = varietyTypeOf(value);
		var finalvalue = { types : [ valueType ], occurences : 1};
		emit(key, finalvalue); 
	} 
}
