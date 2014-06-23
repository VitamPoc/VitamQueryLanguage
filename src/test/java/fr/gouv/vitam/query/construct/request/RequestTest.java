package fr.gouv.vitam.query.construct.request;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import fr.gouv.vitam.query.construct.request.BooleanRequest;
import fr.gouv.vitam.query.construct.request.CompareRequest;
import fr.gouv.vitam.query.construct.request.ExistsRequest;
import fr.gouv.vitam.query.construct.request.InRequest;
import fr.gouv.vitam.query.construct.request.MatchRequest;
import fr.gouv.vitam.query.construct.request.MltRequest;
import fr.gouv.vitam.query.construct.request.PathRequest;
import fr.gouv.vitam.query.construct.request.RangeRequest;
import fr.gouv.vitam.query.construct.request.Request;
import fr.gouv.vitam.query.construct.request.SearchRequest;
import fr.gouv.vitam.query.construct.request.TermRequest;
import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;
import fr.gouv.vitam.query.parser.ParserTokens.REQUESTARGS;

public class RequestTest {
    
    @Test
    public void testRequestBoolean() {
        Request arg1, arg2, argIncomplete;
        try {
            arg1 = new ExistsRequest(REQUEST.exists, "var");
            arg2 = new ExistsRequest(REQUEST.isNull, "var");
            argIncomplete = new BooleanRequest(REQUEST.and);
        } catch (InvalidCreateOperationException e1) {
            e1.printStackTrace();
            fail(e1.getMessage());
            return;
        }
        REQUEST booleanRequest = REQUEST.and;
        try {
            BooleanRequest request = new BooleanRequest(booleanRequest);
            assertFalse(request.isReady());
            request.addToBooleanRequest(arg1).addToBooleanRequest(arg2).addToBooleanRequest(arg1, arg2);
            assertTrue(request.isReady());
            assertEquals(4, request.getCurrentObject().size());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        booleanRequest = REQUEST.or;
        try {
            BooleanRequest request = new BooleanRequest(booleanRequest);
            assertFalse(request.isReady());
            request.addToBooleanRequest(arg1);
            assertTrue(request.isReady());
            request.addToBooleanRequest(arg2);
            request.addToBooleanRequest(arg1, arg2);
            assertTrue(request.isReady());
            assertEquals(4, request.getCurrentObject().size());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        booleanRequest = REQUEST.nor;
        try {
            BooleanRequest request = new BooleanRequest(booleanRequest);
            assertFalse(request.isReady());
            request.addToBooleanRequest(arg1);
            assertTrue(request.isReady());
            request.addToBooleanRequest(arg2);
            request.addToBooleanRequest(arg1, arg2);
            assertTrue(request.isReady());
            assertEquals(4, request.getCurrentObject().size());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        booleanRequest = REQUEST.not;
        try {
            BooleanRequest request = new BooleanRequest(booleanRequest);
            assertFalse(request.isReady());
            request.addToBooleanRequest(arg1);
            assertTrue(request.isReady());
            request.addToBooleanRequest(arg2);
            request.addToBooleanRequest(arg1, arg2);
            assertTrue(request.isReady());
            assertEquals(4, request.getCurrentObject().size());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        // Failed tests
        booleanRequest = REQUEST.and;
        BooleanRequest request = null;
        try {
            request = new BooleanRequest(booleanRequest);
            assertFalse(request.isReady());
            request.addToBooleanRequest(arg1);
            assertTrue(request.isReady());
            assertEquals(1, request.getCurrentObject().size());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        try {
            request.addToBooleanRequest(argIncomplete);
            fail("Should have raized an exception due to incomplete argument");
        } catch (InvalidCreateOperationException e) {
            assertEquals(1, request.getCurrentObject().size());
        }
        // last
        try {
            request = new BooleanRequest(REQUEST.eq);
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testRequestPath() {
        PathRequest request = null;
        try {
            request = new PathRequest("id1", "id2", "id3");
            assertTrue(request.isReady());
            assertEquals(3, request.getCurrentObject().size());
            request.addPath("id4", "id5").addPath("id6");
            assertEquals(6, request.getCurrentObject().size());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        try {
            request = new PathRequest("");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testRequestExists() {
        ExistsRequest request = null;
        try {
            request = new ExistsRequest(REQUEST.exists, "var");
            assertTrue(request.isReady());
            request = new ExistsRequest(REQUEST.missing, "var");
            assertTrue(request.isReady());
            request = new ExistsRequest(REQUEST.isNull, "var");
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        // last
        try {
            request = new ExistsRequest(REQUEST.and, "var");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            request = new ExistsRequest(REQUEST.exists, "");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testRequestCompareLong() {
        CompareRequest request = null;
        try {
            request = new CompareRequest(REQUEST.lt, "var", 1);
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.lte, "var", 1);
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.gt, "var", 1);
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.gte, "var", 1);
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.eq, "var", 1);
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.ne, "var", 1);
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.size, "var", 1);
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        // last
        try {
            request = new CompareRequest(REQUEST.and, "var", 1);
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            request = new CompareRequest(REQUEST.lt, "", 1);
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testRequestCompareDouble() {
        CompareRequest request = null;
        try {
            request = new CompareRequest(REQUEST.lt, "var", 1.0);
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.lte, "var", 1.0);
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.gt, "var", 1.0);
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.gte, "var", 1.0);
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.eq, "var", 1.0);
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.ne, "var", 1.0);
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        // last
        try {
            request = new CompareRequest(REQUEST.size, "var", 1.0);
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            request = new CompareRequest(REQUEST.lt, "", 1.0);
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testRequestREQUESTCompareString() {
        CompareRequest request = null;
        try {
            request = new CompareRequest(REQUEST.lt, "var", "val");
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.lte, "var", "val");
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.gt, "var", "val");
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.gte, "var", "val");
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.eq, "var", "val");
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.ne, "var", "val");
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        // last
        try {
            request = new CompareRequest(REQUEST.size, "var", "val");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            request = new CompareRequest(REQUEST.lt, "", "val");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testRequestREQUESTSearch() {
        SearchRequest request = null;
        try {
            request = new SearchRequest(REQUEST.regex, "var", "val");
            assertTrue(request.isReady());
            request = new SearchRequest(REQUEST.search, "var", "val");
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        // last
        try {
            request = new SearchRequest(REQUEST.size, "var", "val");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            request = new SearchRequest(REQUEST.search, "", "val");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
    }
    @Test
    public void testRequestMatch() {
        MatchRequest request = null;
        try {
            request = new MatchRequest(REQUEST.match, "var", "val");
            assertTrue(request.isReady());
            request.setMatchMaxExpansions(10);
            assertTrue(request.getCurrentObject().has(REQUESTARGS.max_expansions.exactToken()));
            request = new MatchRequest(REQUEST.match_phrase, "var", "val");
            assertTrue(request.isReady());
            request.setMatchMaxExpansions(10);
            assertTrue(request.getCurrentObject().has(REQUESTARGS.max_expansions.exactToken()));
            request = new MatchRequest(REQUEST.match_phrase_prefix, "var", "val");
            assertTrue(request.isReady());
            request.setMatchMaxExpansions(10);
            assertTrue(request.getCurrentObject().has(REQUESTARGS.max_expansions.exactToken()));
            request = new MatchRequest(REQUEST.prefix, "var", "val");
            assertTrue(request.isReady());
            request.setMatchMaxExpansions(10);
            assertTrue(request.getCurrentObject().has(REQUESTARGS.max_expansions.exactToken()));
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        // last
        try {
            request = new MatchRequest(REQUEST.size, "var", "val");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            request = new MatchRequest(REQUEST.match, "", "val");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
    }
    @Test
    public void testRequestIn() {
        InRequest request = null;
        try {
            request = new InRequest(REQUEST.in, "var", true);
            assertTrue(request.isReady());
            request = new InRequest(REQUEST.nin, "var", true);
            assertTrue(request.isReady());
            request = new InRequest(REQUEST.in, "var", 1);
            assertTrue(request.isReady());
            request = new InRequest(REQUEST.nin, "var", 1);
            assertTrue(request.isReady());
            request = new InRequest(REQUEST.in, "var", 1.0);
            assertTrue(request.isReady());
            request = new InRequest(REQUEST.nin, "var", 1.0);
            assertTrue(request.isReady());
            request = new InRequest(REQUEST.in, "var", "val");
            assertTrue(request.isReady());
            request = new InRequest(REQUEST.nin, "var", "val");
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        // last
        try {
            request = new InRequest(REQUEST.size, "var", "val");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            request = new InRequest(REQUEST.in, "", true);
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            request = new InRequest(REQUEST.in, "", 1);
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            request = new InRequest(REQUEST.in, "", 1.0);
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            request = new InRequest(REQUEST.in, "", "val");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
    }
    @Test
    public void testRequestMlt() {
        MltRequest request = null;
        try {
            request = new MltRequest(REQUEST.mlt, "var", "val");
            assertTrue(request.isReady());
            request = new MltRequest(REQUEST.flt, "var", "val");
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        // last
        try {
            request = new MltRequest(REQUEST.size, "var", "val");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            request = new MltRequest(REQUEST.mlt, "", "val");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testRequestREQUESTCompareBoolean() {
        CompareRequest request = null;
        try {
            request = new CompareRequest(REQUEST.lt, "var", true);
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.lte, "var", true);
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.gt, "var", true);
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.gte, "var", true);
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.eq, "var", true);
            assertTrue(request.isReady());
            request = new CompareRequest(REQUEST.ne, "var", true);
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        // last
        try {
            request = new CompareRequest(REQUEST.size, "var", true);
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            request = new CompareRequest(REQUEST.lt, "", true);
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testRequestREQUESTStringStringArray() {
        try {
            InRequest request = null;
            request = new InRequest(REQUEST.in, "var", "val1", "val2");
            assertTrue(request.isReady());
            assertEquals(2, request.getCurrentObject().size());
            request = new InRequest(REQUEST.nin, "var", "val1", "val2");
            assertTrue(request.isReady());
            assertEquals(2, request.getCurrentObject().size());
            request.addInValue("val1", "val2").addInValue("val3");
            assertTrue(request.isReady());
            assertEquals(3, request.getCurrentObject().size());
            request.addInValue(1).addInValue(1.0);
            assertTrue(request.isReady());
            assertEquals(5, request.getCurrentObject().size());
            request = new InRequest(REQUEST.in, "var", 1, 2);
            assertTrue(request.isReady());
            assertEquals(2, request.getCurrentObject().size());
            request = new InRequest(REQUEST.nin, "var", 1, 2);
            assertTrue(request.isReady());
            assertEquals(2, request.getCurrentObject().size());
            request.addInValue("val1", "val2").addInValue("val3");
            assertTrue(request.isReady());
            assertEquals(5, request.getCurrentObject().size());
            request.addInValue(1).addInValue(1.0);
            assertTrue(request.isReady());
            assertEquals(6, request.getCurrentObject().size());
            request = new InRequest(REQUEST.in, "var", 1.0, 2.0);
            assertTrue(request.isReady());
            assertEquals(2, request.getCurrentObject().size());
            request = new InRequest(REQUEST.nin, "var", 1.0, 2.0);
            assertTrue(request.isReady());
            assertEquals(2, request.getCurrentObject().size());
            request.addInValue("val1", "val2").addInValue("val3");
            assertTrue(request.isReady());
            assertEquals(5, request.getCurrentObject().size());
            request.addInValue(1).addInValue(1.0);
            assertTrue(request.isReady());
            assertEquals(6, request.getCurrentObject().size());
            request = new InRequest(REQUEST.in, "var", true, false);
            assertTrue(request.isReady());
            assertEquals(2, request.getCurrentObject().size());
            request = new InRequest(REQUEST.nin, "var", true, false);
            assertTrue(request.isReady());
            assertEquals(2, request.getCurrentObject().size());
            request.addInValue("val1", "val2").addInValue("val3");
            assertTrue(request.isReady());
            assertEquals(5, request.getCurrentObject().size());
            request.addInValue(1).addInValue(1.0);
            assertTrue(request.isReady());
            assertEquals(7, request.getCurrentObject().size());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        // last
        try {
            @SuppressWarnings("unused")
            InRequest request = new InRequest(REQUEST.and, "var", "val1", "val2");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            MltRequest request = new MltRequest(REQUEST.mlt, "value", "var1", "var2");
            assertTrue(request.isReady());
            assertEquals(2, request.getCurrentObject().size());
            request = new MltRequest(REQUEST.flt, "value", "var1", "var2");
            assertTrue(request.isReady());
            assertEquals(2, request.getCurrentObject().size());
            request.addMltVariable("var1", "var2").addMltVariable("var3");
            assertTrue(request.isReady());
            assertEquals(3, request.getCurrentObject().size());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        // last
        try {
            @SuppressWarnings("unused")
            MltRequest request = new MltRequest(REQUEST.and, "var", "val1", "val2");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            @SuppressWarnings("unused")
            MltRequest request = new MltRequest(REQUEST.mlt, "", "val1", "val2");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testRequestTerm() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("var1", "val1");
        map.put("var2", "val2");
        TermRequest request = null;
        try {
            request = new TermRequest("var", "val");
            assertTrue(request.isReady());
            request = new TermRequest(map);
            assertEquals(2, request.getCurrentObject().size());
            assertTrue(request.isReady());
            request.addTermRequest("var2", "val2");
            assertTrue(request.isReady());
            assertEquals(2, request.getCurrentObject().size());
            request.addTermRequest("var3", "val2");
            assertTrue(request.isReady());
            assertEquals(3, request.getCurrentObject().size());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        try {
            request = new TermRequest("", "val1");
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testRequestRange() {
        RangeRequest request = null;
        try {
            request = new RangeRequest("var", REQUEST.gt, 1, REQUEST.lt, 2);
            assertTrue(request.isReady());
            request = new RangeRequest("var", REQUEST.gte, 1, REQUEST.lte, 2);
            assertTrue(request.isReady());
            request = new RangeRequest("var", REQUEST.gt, 1.0, REQUEST.lt, 2.0);
            assertTrue(request.isReady());
            request = new RangeRequest("var", REQUEST.gte, 1.0, REQUEST.lte, 2.0);
            assertTrue(request.isReady());
            request = new RangeRequest("var", REQUEST.gt, "1", REQUEST.lt, "2");
            assertTrue(request.isReady());
            request = new RangeRequest("var", REQUEST.gte, "1", REQUEST.lte, "2");
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        // last
        try {
            request = new RangeRequest("var", REQUEST.not, 1, REQUEST.lt, 2);
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            request = new RangeRequest("var", REQUEST.lt, 1, REQUEST.lt, 2);
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            request = new RangeRequest("var", REQUEST.lt, 1, REQUEST.gt, 2);
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            request = new RangeRequest("var", REQUEST.gt, 1, REQUEST.not, 2);
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
        try {
            request = new RangeRequest("", REQUEST.gt, 1, REQUEST.lt, 2);
            fail("Should have raized an exception due to incorrect argument");
        } catch (InvalidCreateOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testSetExactDepthLimit() {
        TermRequest request = null;
        try {
            request = new TermRequest("var", "val");
            assertEquals(1, request.getCurrentRequest().size());
            assertTrue(request.isReady());
            assertFalse(request.getCurrentRequest().has(REQUESTARGS.depth.exactToken()));
            assertFalse(request.getCurrentRequest().has(REQUESTARGS.relativedepth.exactToken()));
            request.setExactDepthLimit(1);
            assertFalse(request.getCurrentRequest().has(REQUESTARGS.depth.exactToken()));
            assertFalse(request.getCurrentRequest().has(REQUESTARGS.relativedepth.exactToken()));
            request.setExactDepthLimit(0);
            assertTrue(request.getCurrentRequest().has(REQUESTARGS.depth.exactToken()));
            assertFalse(request.getCurrentRequest().has(REQUESTARGS.relativedepth.exactToken()));
            request.setExactDepthLimit(1);
            assertFalse(request.getCurrentRequest().has(REQUESTARGS.depth.exactToken()));
            assertFalse(request.getCurrentRequest().has(REQUESTARGS.relativedepth.exactToken()));
            request.setExactDepthLimit(0);
            assertTrue(request.getCurrentRequest().has(REQUESTARGS.depth.exactToken()));
            assertFalse(request.getCurrentRequest().has(REQUESTARGS.relativedepth.exactToken()));
            request.setRelativeDepthLimit(0);
            assertFalse(request.getCurrentRequest().has(REQUESTARGS.depth.exactToken()));
            assertTrue(request.getCurrentRequest().has(REQUESTARGS.relativedepth.exactToken()));
            request.setRelativeDepthLimit(1);
            assertFalse(request.getCurrentRequest().has(REQUESTARGS.depth.exactToken()));
            assertTrue(request.getCurrentRequest().has(REQUESTARGS.relativedepth.exactToken()));
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

}
