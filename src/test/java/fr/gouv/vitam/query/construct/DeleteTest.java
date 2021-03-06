package fr.gouv.vitam.query.construct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.construct.request.BooleanRequest;
import fr.gouv.vitam.query.construct.request.ExistsRequest;
import fr.gouv.vitam.query.construct.request.PathRequest;
import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.ACTIONFILTER;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;

@SuppressWarnings("javadoc")
public class DeleteTest {

    @Test
    public void testSetMult() {
        final Delete delete = new Delete();
        assertTrue(delete.getFilter().size() == 0);
        delete.setMult(true);
        assertTrue(delete.getFilter().size() == 1);
        delete.setMult(false);
        assertTrue(delete.getFilter().size() == 1);
        assertTrue(delete.filter.has(ACTIONFILTER.mult.exactToken()));
        delete.resetFilter();
        assertTrue(delete.getFilter().size() == 0);
    }

    @Test
    public void testAddRequests() {
        final Delete delete = new Delete();
        assertNull(delete.requests);
        try {
            delete.addRequests(new BooleanRequest(REQUEST.and).addToBooleanRequest(new ExistsRequest(REQUEST.exists, "varA"))
                    .setRelativeDepthLimit(5));
            delete.addRequests(new PathRequest("path1", "path2"),
                    new ExistsRequest(REQUEST.exists, "varB").setExactDepthLimit(10));
            delete.addRequests(new PathRequest("path3"));
            assertEquals(4, delete.requests.size());
            delete.resetRequests();
            assertEquals(0, delete.requests.size());
        } catch (final InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testGetFinalDelete() {
        final Delete delete = new Delete();
        assertNull(delete.requests);
        try {
            delete.addRequests(new PathRequest("path3"));
            assertEquals(1, delete.requests.size());
            delete.setMult(true);
            final ObjectNode node = delete.getFinalDelete();
            assertEquals(2, node.size());
        } catch (final InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

}
