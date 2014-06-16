package fr.gouv.vitam.query.construct;

import static org.junit.Assert.*;

import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.construct.request.BooleanRequest;
import fr.gouv.vitam.query.construct.request.ExistsRequest;
import fr.gouv.vitam.query.construct.request.PathRequest;
import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.ACTIONFILTER;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;

public class DeleteTest {

	@Test
	public void testSetMult() {
		Delete update = new Delete();
		assertTrue(update.getFilter().size() == 0);
		update.setMult(true);
		assertTrue(update.getFilter().size() == 1);
		update.setMult(false);
		assertTrue(update.getFilter().size() == 1);
		assertTrue(update.filter.has(ACTIONFILTER.mult.exactToken()));
		update.resetFilter();
		assertTrue(update.getFilter().size() == 0);
	}

	@Test
	public void testAddRequests() {
		Delete update = new Delete();
		assertNull(update.requests);
		try {
			update.addRequests(new BooleanRequest(REQUEST.and).addToBooleanRequest(new ExistsRequest(REQUEST.exists, "varA")).setRelativeDepthLimit(5));
			update.addRequests(new PathRequest("path1", "path2"),new ExistsRequest(REQUEST.exists, "varB").setExactDepthLimit(10));
			update.addRequests(new PathRequest("path3"));
			assertEquals(4, update.requests.size());
		} catch (InvalidCreateOperationException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testGetFinalDelete() {
		Delete update = new Delete();
		assertNull(update.requests);
		try {
			update.addRequests(new PathRequest("path3"));
			assertEquals(1, update.requests.size());
			update.setMult(true);
			ObjectNode node = update.getFinalDelete();
			assertEquals(2, node.size());
		} catch (InvalidCreateOperationException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
