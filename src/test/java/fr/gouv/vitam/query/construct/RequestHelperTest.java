package fr.gouv.vitam.query.construct;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import fr.gouv.vitam.query.construct.request.Request;
import fr.gouv.vitam.query.exception.InvalidCreateOperationException;

@SuppressWarnings("javadoc")
public class RequestHelperTest {

    @Test
    public void testPathRequest() {
        try {
            final Request request = RequestHelper.path("id1", "id2");
            assertTrue(request.isReady());
        } catch (final InvalidCreateOperationException e) {
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
        } catch (final InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCompareRequest() {
        Date date1 = new Date(System.currentTimeMillis());
        try {
            Request request = RequestHelper.eq("var", true);
            assertTrue(request.isReady());
            request = RequestHelper.eq("var", 1);
            assertTrue(request.isReady());
            request = RequestHelper.eq("var", 1.0);
            assertTrue(request.isReady());
            request = RequestHelper.eq("var", "val");
            assertTrue(request.isReady());
            request = RequestHelper.eq("var", date1);
            assertTrue(request.isReady());

            request = RequestHelper.ne("var", true);
            assertTrue(request.isReady());
            request = RequestHelper.ne("var", 1);
            assertTrue(request.isReady());
            request = RequestHelper.ne("var", 1.0);
            assertTrue(request.isReady());
            request = RequestHelper.ne("var", "val");
            assertTrue(request.isReady());
            request = RequestHelper.ne("var", date1);
            assertTrue(request.isReady());

            request = RequestHelper.gt("var", true);
            assertTrue(request.isReady());
            request = RequestHelper.gt("var", 1);
            assertTrue(request.isReady());
            request = RequestHelper.gt("var", 1.0);
            assertTrue(request.isReady());
            request = RequestHelper.gt("var", "val");
            assertTrue(request.isReady());
            request = RequestHelper.gt("var", date1);
            assertTrue(request.isReady());

            request = RequestHelper.gte("var", true);
            assertTrue(request.isReady());
            request = RequestHelper.gte("var", 1);
            assertTrue(request.isReady());
            request = RequestHelper.gte("var", 1.0);
            assertTrue(request.isReady());
            request = RequestHelper.gte("var", "val");
            assertTrue(request.isReady());
            request = RequestHelper.gte("var", date1);
            assertTrue(request.isReady());

            request = RequestHelper.lt("var", true);
            assertTrue(request.isReady());
            request = RequestHelper.lt("var", 1);
            assertTrue(request.isReady());
            request = RequestHelper.lt("var", 1.0);
            assertTrue(request.isReady());
            request = RequestHelper.lt("var", "val");
            assertTrue(request.isReady());
            request = RequestHelper.lt("var", date1);
            assertTrue(request.isReady());

            request = RequestHelper.lte("var", true);
            assertTrue(request.isReady());
            request = RequestHelper.lte("var", 1);
            assertTrue(request.isReady());
            request = RequestHelper.lte("var", 1.0);
            assertTrue(request.isReady());
            request = RequestHelper.lte("var", "val");
            assertTrue(request.isReady());
            request = RequestHelper.lte("var", date1);
            assertTrue(request.isReady());

            request = RequestHelper.size("var", 1);
            assertTrue(request.isReady());
        } catch (final InvalidCreateOperationException e) {
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
        } catch (final InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testInRequest() {
        Date date1 = new Date(System.currentTimeMillis());
        Date date2 = new Date(System.currentTimeMillis()+100);
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
            request = RequestHelper.in("var", date1);
            assertTrue(request.isReady());
            request = RequestHelper.in("var", date2);
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
            request = RequestHelper.nin("var", date1);
            assertTrue(request.isReady());
            request = RequestHelper.nin("var", date2);
            assertTrue(request.isReady());
        } catch (final InvalidCreateOperationException e) {
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
        } catch (final InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testTermRequest() {
        try {
            Request request = RequestHelper.term("var", "value");
            assertTrue(request.isReady());
            final Map<String, Object> map = new HashMap<String, Object>();
            map.put("var1", "val1");
            map.put("var2", "val2");
            map.put("var3", new Date(0));
            map.put("var4", 1);
            map.put("var5", 2.0);
            map.put("var6", true);
            request = RequestHelper.term(map);
            assertTrue(request.isReady());
        } catch (final InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testWildcardRequest() {
        try {
            Request request = RequestHelper.wildcard("var", "value");
            assertTrue(request.isReady());
        } catch (final InvalidCreateOperationException e) {
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
        } catch (final InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testRangeRequest() {
        Date date1 = new Date(System.currentTimeMillis());
        Date date2 = new Date(System.currentTimeMillis()+100);
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
            
            request = RequestHelper.range("var", date1, false, date2, false);
            assertTrue(request.isReady());
            request = RequestHelper.range("var", date1, true, date2, false);
            assertTrue(request.isReady());
            request = RequestHelper.range("var", date1, false, date2, true);
            assertTrue(request.isReady());
            request = RequestHelper.range("var", date1, true, date2, true);
            assertTrue(request.isReady());
        } catch (final InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
