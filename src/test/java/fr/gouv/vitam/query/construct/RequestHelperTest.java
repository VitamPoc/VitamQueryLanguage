package fr.gouv.vitam.query.construct;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import fr.gouv.vitam.query.construct.RequestHelper;
import fr.gouv.vitam.query.construct.request.Request;
import fr.gouv.vitam.query.exception.InvalidCreateOperationException;

public class RequestHelperTest {

    @Test
    public void testPathRequest() {
        try {
            Request request = RequestHelper.path("id1", "id2");
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testBoolRequest() {
        try {
            Request request = RequestHelper.and().addToBooleanRequest(RequestHelper.eq("var", "val"));
            assertTrue(request.isReady());
            request = RequestHelper.or().addToBooleanRequest(RequestHelper.eq("var", "val"));
            assertTrue(request.isReady());
            request = RequestHelper.not().addToBooleanRequest(RequestHelper.eq("var", "val"));
            assertTrue(request.isReady());
            request = RequestHelper.not().addToBooleanRequest(RequestHelper.eq("var", "val"));
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCompareRequestStringBoolean() {
        try {
            Request request = RequestHelper.eq("var", true);
            assertTrue(request.isReady());
            request = RequestHelper.eq("var", 1);
            assertTrue(request.isReady());
            request = RequestHelper.eq("var", 1.0);
            assertTrue(request.isReady());
            request = RequestHelper.eq("var", "val");
            assertTrue(request.isReady());
            
            request = RequestHelper.ne("var", true);
            assertTrue(request.isReady());
            request = RequestHelper.ne("var", 1);
            assertTrue(request.isReady());
            request = RequestHelper.ne("var", 1.0);
            assertTrue(request.isReady());
            request = RequestHelper.ne("var", "val");
            assertTrue(request.isReady());
            
            request = RequestHelper.gt("var", true);
            assertTrue(request.isReady());
            request = RequestHelper.gt("var", 1);
            assertTrue(request.isReady());
            request = RequestHelper.gt("var", 1.0);
            assertTrue(request.isReady());
            request = RequestHelper.gt("var", "val");
            assertTrue(request.isReady());
            
            request = RequestHelper.gte("var", true);
            assertTrue(request.isReady());
            request = RequestHelper.gte("var", 1);
            assertTrue(request.isReady());
            request = RequestHelper.gte("var", 1.0);
            assertTrue(request.isReady());
            request = RequestHelper.gte("var", "val");
            assertTrue(request.isReady());
            
            request = RequestHelper.lt("var", true);
            assertTrue(request.isReady());
            request = RequestHelper.lt("var", 1);
            assertTrue(request.isReady());
            request = RequestHelper.lt("var", 1.0);
            assertTrue(request.isReady());
            request = RequestHelper.lt("var", "val");
            assertTrue(request.isReady());

            request = RequestHelper.lte("var", true);
            assertTrue(request.isReady());
            request = RequestHelper.lte("var", 1);
            assertTrue(request.isReady());
            request = RequestHelper.lte("var", 1.0);
            assertTrue(request.isReady());
            request = RequestHelper.lte("var", "val");
            assertTrue(request.isReady());

            request = RequestHelper.size("var", 1);
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testExistsRequest() {
        try {
            Request request = RequestHelper.exists("var");
            assertTrue(request.isReady());
            
            request = RequestHelper.missing("var");
            assertTrue(request.isReady());
            
            request = RequestHelper.isNull("var");
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testInRequestStringBooleanArray() {
        try {
            Request request = RequestHelper.in("var", true);
            assertTrue(request.isReady());
            request = RequestHelper.in("var", true, true);
            assertTrue(request.isReady());
            request = RequestHelper.in("var", 1);
            assertTrue(request.isReady());
            request = RequestHelper.in("var", 1, 1);
            assertTrue(request.isReady());
            request = RequestHelper.in("var", 1.0);
            assertTrue(request.isReady());
            request = RequestHelper.in("var", 1.0, 1.0);
            assertTrue(request.isReady());
            request = RequestHelper.in("var", "val");
            assertTrue(request.isReady());
            request = RequestHelper.in("var", "val", "val");
            assertTrue(request.isReady());
            
            request = RequestHelper.nin("var", true);
            assertTrue(request.isReady());
            request = RequestHelper.nin("var", true, true);
            assertTrue(request.isReady());
            request = RequestHelper.nin("var", 1);
            assertTrue(request.isReady());
            request = RequestHelper.nin("var", 1, 1);
            assertTrue(request.isReady());
            request = RequestHelper.nin("var", 1.0);
            assertTrue(request.isReady());
            request = RequestHelper.nin("var", 1.0, 1.0);
            assertTrue(request.isReady());
            request = RequestHelper.nin("var", "val");
            assertTrue(request.isReady());
            request = RequestHelper.nin("var", "val", "val");
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testMatchRequest() {
        try {
            Request request = RequestHelper.match("var", "value");
            assertTrue(request.isReady());
            request = RequestHelper.matchPhrase("var", "value");
            assertTrue(request.isReady());
            request = RequestHelper.matchPhrasePrefix("var", "value");
            assertTrue(request.isReady());
            request = RequestHelper.prefix("var", "value");
            assertTrue(request.isReady());
            request = RequestHelper.regex("var", "value");
            assertTrue(request.isReady());
            request = RequestHelper.search("var", "value");
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testTermRequestStringString() {
        try {
            Request request = RequestHelper.term("var", "value");
            assertTrue(request.isReady());
            Map<String, String> map = new HashMap<String, String>();
            map.put("var1", "val1");
            map.put("var2", "val2");
            request = RequestHelper.term(map);
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testFltRequest() {
        try {
            Request request = RequestHelper.flt("value", "var");
            assertTrue(request.isReady());
            request = RequestHelper.flt("value", "var1", "var2");
            assertTrue(request.isReady());
            
            request = RequestHelper.mlt("value", "var");
            assertTrue(request.isReady());
            request = RequestHelper.mlt("value", "var1", "var2");
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testRangeRequestStringLongBooleanLongBoolean() {
        try {
            Request request = RequestHelper.range("var", 1, false, 2, false);
            assertTrue(request.isReady());
            request = RequestHelper.range("var", 1, true, 2, false);
            assertTrue(request.isReady());
            request = RequestHelper.range("var", 1, false, 2, true);
            assertTrue(request.isReady());
            request = RequestHelper.range("var", 1, true, 2, true);
            assertTrue(request.isReady());
            
            request = RequestHelper.range("var", 1.0, false, 2.0, false);
            assertTrue(request.isReady());
            request = RequestHelper.range("var", 1.0, true, 2.0, false);
            assertTrue(request.isReady());
            request = RequestHelper.range("var", 1.0, false, 2.0, true);
            assertTrue(request.isReady());
            request = RequestHelper.range("var", 1.0, true, 2.0, true);
            assertTrue(request.isReady());
            
            request = RequestHelper.range("var", "1", false, "2", false);
            assertTrue(request.isReady());
            request = RequestHelper.range("var", "1", true, "2", false);
            assertTrue(request.isReady());
            request = RequestHelper.range("var", "1", false, "2", true);
            assertTrue(request.isReady());
            request = RequestHelper.range("var", "1", true, "2", true);
            assertTrue(request.isReady());
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
