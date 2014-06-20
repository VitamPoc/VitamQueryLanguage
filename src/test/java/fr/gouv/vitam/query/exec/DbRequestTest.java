/**
   This file is part of Vitam Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All Vitam Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   Vitam is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Vitam .  If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.query.exec;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import fr.gouv.vitam.mdbtypes.ResultCached;
import fr.gouv.vitam.query.exception.InvalidExecOperationException;
import fr.gouv.vitam.query.exception.InvalidParseOperationException;
import fr.gouv.vitam.query.parser.MdEsQueryParser;
import fr.gouv.vitam.utils.UUID;

/**
 * @author frederic
 *
 */
public class DbRequestTest {
	private static final String exampleBothEsMd = 
			"{ $query : [ { $path : [ 'id1', 'id2'] },"+
			    "{ $and : [ {$exists : 'mavar1'}, {$missing : 'mavar2'}, {$isNull : 'mavar3'}, { $or : [ {$in : { 'mavar4' : [1, 2, 'maval1'] }}, { $nin : { 'mavar5' : ['maval2', true] } } ] } ] },"+
			    "{ $not : [ { $size : { 'mavar5' : 5 } }, { $gt : { 'mavar6' : 7 } }, { $lte : { 'mavar7' : 8 } } ] , $depth : 5},"+
			    "{ $nor : [ { $eq : { 'mavar8' : 5 } }, { $ne : { 'mavar9' : 'ab' } }, { $range : { 'mavar10' : { $gte : 12, $lte : 20} } } ], $relativedepth : 5},"+
			    "{ $match_phrase : { 'mavar11' : 'ceci est une phrase' }, $relativedepth : 0},"+
			    "{ $match_phrase_prefix : { 'mavar11' : 'ceci est une phrase', $max_expansions : 10 }, $relativedepth : 0},"+
			    "{ $flt : { $fields : [ 'mavar12', 'mavar13' ], $like : 'ceci est une phrase' }, $relativedepth : 1},"+
			    "{ $and : [ {$search : { 'mavar13' : 'ceci est une phrase' } }, {$regex : { 'mavar14' : '^start?aa.*' } } ] },"+
			    "{ $and : [ { $term : { 'mavar14' : 'motMajuscule', 'mavar15' : 'simplemot' } } ] },"+
			    "{ $and : [ { $term : { 'mavar16' : 'motMajuscule', 'mavar17' : 'simplemot' } }, { $or : [ {$eq : { 'mavar19' : 'abcd' } }, { $match : { 'mavar18' : 'quelques mots' } } ] } ] },"+
			    "{ $regex : { 'mavar14' : '^start?aa.*' } }"+
			    "], "+
			    "$filter : {$offset : 100, $limit : 1000, $hint : ['cache'], $orderby : { maclef1 : 1 , maclef2 : -1,  maclef3 : 1 } },"+ 
			    "$projection : {$fields : {@dua : 1, @all : 1}, $usage : 'abcdef1234' } }";

	/**
	 * Test method for {@link fr.gouv.vitam.query.exec.DbRequest#execQuery(fr.gouv.vitam.query.parser.AbstractQueryParser, fr.gouv.vitam.mdbtypes.ResultCached)}.
	 */
	@Test
	public void testExecQuery() {
		DbRequest dbRequest = new DbRequest();
		MdEsQueryParser query = new MdEsQueryParser(true);
		try {
			query.parse(exampleBothEsMd);
		} catch (InvalidParseOperationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		ResultCached startSet = new ResultCached();
		startSet.currentMaip.add(new UUID().toString());
		startSet.minLevel = 1;
		startSet.maxLevel = 1;
		startSet.loaded = true;
		startSet.nbSubNodes = 10;
		startSet.putBeforeSave();
		try {
			List<ResultCached> results = dbRequest.execQuery(query, startSet);
			assertFalse(results.isEmpty());
			int i = 0;
			for (ResultCached resultCached : results) {
				resultCached.putBeforeSave();
				i++;
				System.out.println("Level "+i+": "+resultCached);
				assertFalse(resultCached.currentMaip.isEmpty());
				System.out.println("Level: "+i+" : "+resultCached.currentMaip);
			}
			ResultCached result = dbRequest.finalizeResults(results);
			result.putBeforeSave();
			System.out.println("Final: "+result);
			assertFalse(result.currentMaip.isEmpty());
			System.out.println("Final: "+result.currentMaip);
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail(e.getMessage());
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InvalidExecOperationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail(e.getMessage());
		}
		assertTrue("Not yet implemented", true);
	}

	/**
	 * Test method for {@link fr.gouv.vitam.query.exec.DbRequest#finalizeResults(java.util.List)}.
	 */
	@Test
	public void testFinalizeResults() {
		assertTrue("Not yet implemented", true);
	}

}
