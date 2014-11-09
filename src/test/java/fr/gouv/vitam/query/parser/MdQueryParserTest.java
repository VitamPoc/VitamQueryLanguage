package fr.gouv.vitam.query.parser;

import static fr.gouv.vitam.query.construct.RequestHelper.and;
import static fr.gouv.vitam.query.construct.RequestHelper.eq;
import static fr.gouv.vitam.query.construct.RequestHelper.gt;
import static fr.gouv.vitam.query.construct.RequestHelper.lte;
import static fr.gouv.vitam.query.construct.RequestHelper.ne;
import static fr.gouv.vitam.query.construct.RequestHelper.nor;
import static fr.gouv.vitam.query.construct.RequestHelper.not;
import static fr.gouv.vitam.query.construct.RequestHelper.range;
import static fr.gouv.vitam.query.construct.RequestHelper.regex;
import static fr.gouv.vitam.query.construct.RequestHelper.size;
import static fr.gouv.vitam.query.construct.RequestHelper.term;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import fr.gouv.vitam.query.construct.Query;
import fr.gouv.vitam.query.construct.request.BooleanRequest;
import fr.gouv.vitam.query.construct.request.ExistsRequest;
import fr.gouv.vitam.query.construct.request.InRequest;
import fr.gouv.vitam.query.construct.request.PathRequest;
import fr.gouv.vitam.query.parser.ParserTokens.FILTERARGS;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;
import fr.gouv.vitam.utils.exception.InvalidParseOperationException;
import fr.gouv.vitam.utils.logging.VitamLogLevel;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

@SuppressWarnings("javadoc")
public class MdQueryParserTest {
    private static final String exampleBothEsMd = "{ $query : [ { $path : [ 'id1', 'id2'] },"
            + "{ $and : [ {$exists : 'mavar1'}, {$missing : 'mavar2'}, {$isNull : 'mavar3'}, { $or : [ {$in : { 'mavar4' : [1, 2, 'maval1'] }}, { $nin : { 'mavar5' : ['maval2', true] } } ] } ] },"
            + "{ $not : [ { $size : { 'mavar5' : 5 } }, { $gt : { 'mavar6' : 7 } }, { $lte : { 'mavar7' : 8 } } ] , $depth : 5},"
            + "{ $nor : [ { $eq : { 'mavar8' : 5 } }, { $ne : { 'mavar9' : 'ab' } }, { $range : { 'mavar10' : { $gte : 12, $lte : 20} } } ], $relativedepth : 5},"
            +
            // "{ $match_phrase : { 'mavar11' : 'ceci est une phrase' }, $relativedepth : 0},"+
            // "{ $match_phrase_prefix : { 'mavar11' : 'ceci est une phrase', $max_expansions : 10 }, $relativedepth : 0},"+
            // "{ $flt : { $fields : [ 'mavar12', 'mavar13' ], $like : 'ceci est une phrase' }, $relativedepth : 1},"+
            // "{ $and : [ {$search : { 'mavar13' : 'ceci est une phrase' } }, {$regex : { 'mavar14' : '^start?aa.*' } } ] },"+
            "{ $and : [ { $term : { 'mavar14' : 'motMajuscule', 'mavar15' : 'simplemot' } } ] },"
            +
            // "{ $and : [ { $term : { 'mavar16' : 'motMajuscule', 'mavar17' : 'simplemot' } }, { $or : [ {$eq : { 'mavar19' : 'abcd' } }, { $match : { 'mavar18' : 'quelques mots' } } ] } ] },"+
            "{ $regex : { 'mavar14' : '^start?aa.*' } }"
            + "], "
            + "$filter : {$offset : 100, $limit : 1000, $hint : ['cache'], $orderby : { maclef1 : 1 , maclef2 : -1,  maclef3 : 1 } },"
            + "$projection : {$fields : {@dua : 1, @all : 1}, $usage : 'abcdef1234' } }";
    private static final String exampleMd = "{ $query : [ { $path : [ 'id1', 'id2'] },"
            + "{ $and : [ {$exists : 'mavar1'}, {$missing : 'mavar2'}, {$isNull : 'mavar3'}, { $or : [ {$in : { 'mavar4' : [1, 2, 'maval1'] }}, { $nin : { 'mavar5' : ['maval2', true] } } ] } ] },"
            + "{ $not : [ { $size : { 'mavar5' : 5 } }, { $gt : { 'mavar6' : 7 } }, { $lte : { 'mavar7' : 8 } } ] , $depth : 4},"
            + "{ $nor : [ { $eq : { 'mavar8' : 5 } }, { $ne : { 'mavar9' : 'ab' } }, { $range : { 'mavar10' : { $gte : 12, $lte : 20} } } ], $relativedepth : 1},"
            +
            // "{ $match_phrase : { 'mavar11' : 'ceci est une phrase' }, $relativedepth : 0},"+
            // "{ $match_phrase_prefix : { 'mavar11' : 'ceci est une phrase', $max_expansions : 10 }, $relativedepth : 0},"+
            // "{ $flt : { $fields : [ 'mavar12', 'mavar13' ], $like : 'ceci est une phrase' }, $relativedepth : 1},"+
            // "{ $and : [ {$search : { 'mavar13' : 'ceci est une phrase' } }, {$regex : { 'mavar14' : '^start?aa.*' } } ] },"+
            "{ $and : [ { $term : { 'mavar14' : 'motMajuscule', 'mavar15' : 'simplemot' } } ] },"
            +
            // "{ $and : [ { $term : { 'mavar16' : 'motMajuscule', 'mavar17' : 'simplemot' } }, { $or : [ {$eq : { 'mavar19' : 'abcd' } }, { $match : { 'mavar18' : 'quelques mots' } } ] } ] },"+
            "{ $regex : { 'mavar14' : '^start?aa.*' } }"
            + "], "
            + "$filter : {$offset : 100, $limit : 1000, $hint : ['cache'], $orderby : { maclef1 : 1 , maclef2 : -1,  maclef3 : 1 } },"
            + "$projection : {$fields : {@dua : 1, @all : 1}, $usage : 'abcdef1234' } }";

    @Before
    public void init() {
        VitamLoggerFactory.setLogLevel(VitamLogLevel.INFO);
    }

    @Test
    public void testParse() {
        try {
            final MdQueryParser command1 = new MdQueryParser(true);
            command1.parse(exampleBothEsMd);
            fail("Should refuse the request since ES is not allowed");
        } catch (final Exception e) {
        }
        try {
            final MdQueryParser command1 = new MdQueryParser(true);
            command1.parse(exampleMd);
            assertNotNull(command1);
            final Query query = new Query();
            query.addRequests(new PathRequest("id1", "id2"));
            query.addRequests(new BooleanRequest(REQUEST.and).addToBooleanRequest(new ExistsRequest(REQUEST.exists, "mavar1"),
                    new ExistsRequest(REQUEST.missing, "mavar2"), new ExistsRequest(REQUEST.isNull, "mavar3"),
                    new BooleanRequest(REQUEST.or).addToBooleanRequest(new InRequest(REQUEST.in, "mavar4", 1, 2)
                    .addInValue("maval1"), new InRequest(REQUEST.nin, "mavar5", "maval2").addInValue(true))));
            query.addRequests(not().addToBooleanRequest(size("mavar5", 5), gt("mavar6", 7), lte("mavar7", 8)).setExactDepthLimit(
                    4));
            query.addRequests(nor()
                    .addToBooleanRequest(eq("mavar8", 5), ne("mavar9", "ab"), range("mavar10", 12, true, 20, true))
                    .setRelativeDepthLimit(1));
            /*
             * query.addRequests(matchPhraseRequest("mavar11", "ceci est une phrase").setRelativeDepthLimit(0));
             * query.addRequests(matchPhrasePrefixRequest("mavar11",
             * "ceci est une phrase").setMatchMaxExpansions(10).setRelativeDepthLimit(0));
             * query.addRequests(fltRequest("ceci est une phrase", "mavar12", "mavar13").setRelativeDepthLimit(1));
             * query.addRequests(andRequest().addToBooleanRequest(searchRequest("mavar13", "ceci est une phrase"),
             * regexRequest("mavar14", "^start?aa.*")));
             */
            query.addRequests(and().addToBooleanRequest(term("mavar14", "motMajuscule").addTermRequest("mavar15", "simplemot")));
            /*
             * query.addRequests(andRequest().addToBooleanRequest(termRequest("mavar16",
             * "motMajuscule").addTermRequest("mavar17","simplemot"),
             * orRequest().addToBooleanRequest(eqRequest("mavar19", "abcd"), matchRequest("mavar18", "quelques mots"))));
             */
            query.addRequests(regex("mavar14", "^start?aa.*"));

            query.setLimitFilter(100, 1000).addHintFilter(FILTERARGS.cache.exactToken()).addOrderByAscFilter("maclef1")
            .addOrderByDescFilter("maclef2").addOrderByAscFilter("maclef3");
            query.addUsedProjection("@dua", "@all").setUsageProjection("abcdef1234");
            final MdQueryParser command = new MdQueryParser(true);
            command.parse(query.getFinalQuery().toString());
            assertNotNull(command);
            final List<TypeRequest> request1 = command1.getRequests();
            final List<TypeRequest> request = command.getRequests();
            for (int i = 0; i < request1.size(); i++) {
                if (!request1.get(i).toString().equals(request.get(i).toString())) {
                    System.err.println(request1.get(i));
                    System.err.println(request.get(i));
                }
                assertTrue("TypeRequest should be equal", request1.get(i).toString().equals(request.get(i).toString()));
            }
            assertTrue("Projection should be equal", command1.projection.toString().equals(command.projection.toString()));
            assertTrue("OrderBy should be equal", command1.orderBy.toString().equals(command.orderBy.toString()));
            assertTrue("ContractId should be equal", command1.contractId.equals(command.contractId));
            assertEquals(command1.hintCache, command.hintCache);
            assertEquals(command1.lastDepth, command.lastDepth);
            assertEquals(command1.limit, command.limit);
            assertEquals(command1.offset, command.offset);
            assertTrue("Command should be equal", command1.toString().equals(command.toString()));
        } catch (final Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testFilterParse() {
        final MdQueryParser command = new MdQueryParser(true);
        final Query query = new Query();
        try {
            // empty
            command.filterParse(query.getFilter());
            assertFalse("Hint should be false", command.hintCache);
            assertEquals(0, command.limit);
            assertEquals(0, command.offset);
            assertNull("OrderBy should be null", command.orderBy);
            // hint set
            query.addHintFilter(FILTERARGS.cache.exactToken());
            command.filterParse(query.getFilter());
            assertTrue("Hint should be True", command.hintCache);
            // hint reset
            query.resetHintFilter();
            command.filterParse(query.getFilter());
            assertFalse("Hint should be false", command.hintCache);
            // hint set false
            query.addHintFilter(FILTERARGS.nocache.exactToken());
            command.filterParse(query.getFilter());
            assertFalse("Hint should be false", command.hintCache);
            // hint unset
            query.resetHintFilter();
            command.filterParse(query.getFilter());
            assertFalse("Hint should be false", command.hintCache);
            // limit set
            query.setLimitFilter(0, 1000);
            command.filterParse(query.getFilter());
            assertEquals(1000, command.limit);
            assertEquals(0, command.offset);
            // offset set
            query.setLimitFilter(100, 0);
            command.filterParse(query.getFilter());
            assertEquals(100, command.offset);
            // orderBy set through array
            query.addOrderByAscFilter("var1", "var2").addOrderByDescFilter("var3");
            command.filterParse(query.getFilter());
            assertNotNull(command.orderBy);
            // check both
            assertEquals(3, command.orderBy.size());
            for (final Iterator<Entry<String, JsonNode>> iterator = command.orderBy.fields(); iterator.hasNext();) {
                final Entry<String, JsonNode> entry = iterator.next();
                if (entry.getKey().equals("var1")) {
                    assertEquals(1, entry.getValue().asInt());
                }
                if (entry.getKey().equals("var2")) {
                    assertEquals(1, entry.getValue().asInt());
                }
                if (entry.getKey().equals("var3")) {
                    assertEquals(-1, entry.getValue().asInt());
                }
            }
            // orderBy set through composite
            query.resetOrderByFilter();
            command.filterParse(query.getFilter());
            assertNull("OrderBy should be null", command.orderBy);
        } catch (final InvalidParseOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testProjectionParse() {
        final MdQueryParser command = new MdQueryParser(true);
        final Query query = new Query();
        try {
            // empty rootNode
            command.projectionParse(query.getProjection());
            assertNull("Projection should be null", command.projection);
            assertNull("ContractId should be null", command.contractId);
            // contractId set
            query.setUsageProjection("abcd");
            command.projectionParse(query.getProjection());
            assertNotNull("ContractId should not be null", command.contractId);
            // projection set but empty
            query.addUsedProjection((String) null);
            // empty set
            command.projectionParse(query.getProjection());
            assertNotNull("Projection should not be null", command.projection);
            assertEquals(0, command.projection.size());
            // not empty set
            query.addUsedProjection("var1").addUnusedProjection("var2");
            command.projectionParse(query.getProjection());
            assertNotNull("Projection should not be null", command.projection);
            assertEquals(2, command.projection.size());
            for (final Iterator<Entry<String, JsonNode>> iterator = command.projection.fields(); iterator.hasNext();) {
                final Entry<String, JsonNode> entry = iterator.next();
                if (entry.getKey().equals("var1")) {
                    assertEquals(1, entry.getValue().asInt());
                }
                if (entry.getKey().equals("var2")) {
                    assertEquals(0, entry.getValue().asInt());
                }
            }
        } catch (final InvalidParseOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
