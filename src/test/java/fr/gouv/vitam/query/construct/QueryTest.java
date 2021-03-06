package fr.gouv.vitam.query.construct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.construct.request.BooleanRequest;
import fr.gouv.vitam.query.construct.request.ExistsRequest;
import fr.gouv.vitam.query.construct.request.PathRequest;
import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.FILTERARGS;
import fr.gouv.vitam.query.parser.ParserTokens.PROJECTION;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;
import fr.gouv.vitam.query.parser.ParserTokens.REQUESTFILTER;

@SuppressWarnings("javadoc")
public class QueryTest {

    @Test
    public void testAddLimitFilter() {
        final Query query = new Query();
        assertNull(query.filter);
        query.setLimitFilter(0, 0);
        assertFalse(query.filter.has(REQUESTFILTER.limit.exactToken()));
        assertFalse(query.filter.has(REQUESTFILTER.offset.exactToken()));
        query.setLimitFilter(1, 0);
        assertFalse(query.filter.has(REQUESTFILTER.limit.exactToken()));
        assertTrue(query.filter.has(REQUESTFILTER.offset.exactToken()));
        query.setLimitFilter(0, 1);
        assertTrue(query.filter.has(REQUESTFILTER.limit.exactToken()));
        assertFalse(query.filter.has(REQUESTFILTER.offset.exactToken()));
        query.setLimitFilter(1, 1);
        assertTrue(query.filter.has(REQUESTFILTER.limit.exactToken()));
        assertTrue(query.filter.has(REQUESTFILTER.offset.exactToken()));
        query.resetLimitFilter();
        assertFalse(query.filter.has(REQUESTFILTER.limit.exactToken()));
        assertFalse(query.filter.has(REQUESTFILTER.offset.exactToken()));
    }

    @Test
    public void testAddHintFilter() {
        final Query query = new Query();
        assertNull(query.filter);
        query.addHintFilter(FILTERARGS.cache.exactToken());
        assertTrue(query.filter.has(REQUESTFILTER.hint.exactToken()));
        assertEquals(1, query.filter.get(REQUESTFILTER.hint.exactToken()).size());
        query.addHintFilter(FILTERARGS.nocache.exactToken());
        assertTrue(query.filter.has(REQUESTFILTER.hint.exactToken()));
        assertEquals(2, query.filter.get(REQUESTFILTER.hint.exactToken()).size());
        query.resetHintFilter();
        assertFalse(query.filter.has(REQUESTFILTER.hint.exactToken()));
    }

    @Test
    public void testAddOrderByAscFilter() {
        final Query query = new Query();
        assertNull(query.filter);
        query.addOrderByAscFilter("var1", "var2");
        assertEquals(2, query.filter.get(REQUESTFILTER.orderby.exactToken()).size());
        query.addOrderByAscFilter("var3").addOrderByAscFilter("var4");
        assertEquals(4, query.filter.get(REQUESTFILTER.orderby.exactToken()).size());
        query.addOrderByDescFilter("var1", "var2");
        assertEquals(4, query.filter.get(REQUESTFILTER.orderby.exactToken()).size());
        query.addOrderByDescFilter("var3").addOrderByDescFilter("var4");
        assertEquals(4, query.filter.get(REQUESTFILTER.orderby.exactToken()).size());
        query.resetOrderByFilter();
        assertFalse(query.filter.has(REQUESTFILTER.orderby.exactToken()));
    }

    @Test
    public void testAddUsedProjection() {
        final Query query = new Query();
        assertNull(query.projection);
        query.addUsedProjection("var1", "var2");
        assertEquals(2, query.projection.get(PROJECTION.fields.exactToken()).size());
        query.addUsedProjection("var3").addUsedProjection("var4");
        assertEquals(4, query.projection.get(PROJECTION.fields.exactToken()).size());
        query.addUnusedProjection("var1", "var2");
        // used/unused identical so don't change the number
        assertEquals(4, query.projection.get(PROJECTION.fields.exactToken()).size());
        query.addUnusedProjection("var3").addUnusedProjection("var4");
        assertEquals(4, query.projection.get(PROJECTION.fields.exactToken()).size());
        query.resetUsedProjection();
        assertFalse(query.projection.has(PROJECTION.fields.exactToken()));
    }

    @Test
    public void testAddUsageProjection() {
        final Query query = new Query();
        assertNull(query.projection);
        query.setUsageProjection("usage");
        assertTrue(query.projection.has(PROJECTION.usage.exactToken()));
        query.resetUsageProjection();
        assertFalse(query.projection.has(PROJECTION.usage.exactToken()));
    }

    @Test
    public void testAddRequests() {
        final Query query = new Query();
        assertNull(query.requests);
        try {
            query.addRequests(new BooleanRequest(REQUEST.and).addToBooleanRequest(new ExistsRequest(REQUEST.exists, "varA"))
                    .setRelativeDepthLimit(5));
            query.addRequests(new PathRequest("path1", "path2"), new ExistsRequest(REQUEST.exists, "varB").setExactDepthLimit(10));
            query.addRequests(new PathRequest("path3"));
            assertEquals(4, query.getRequests().size());
            query.setLimitFilter(10, 10);
            query.addHintFilter(FILTERARGS.cache.exactToken());
            query.addOrderByAscFilter("var1").addOrderByDescFilter("var2");
            query.addUsedProjection("var3").addUnusedProjection("var4");
            query.setUsageProjection("usageId");
            final ObjectNode node = query.getFinalQuery();
            assertEquals(3, node.size());
            query.resetRequests();
            assertEquals(0, query.getRequests().size());
        } catch (final InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

}
