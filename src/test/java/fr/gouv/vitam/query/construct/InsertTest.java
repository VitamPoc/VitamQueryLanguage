package fr.gouv.vitam.query.construct;

import static org.junit.Assert.*;

import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.construct.request.BooleanRequest;
import fr.gouv.vitam.query.construct.request.ExistsRequest;
import fr.gouv.vitam.query.construct.request.PathRequest;
import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.json.JsonHandler;
import fr.gouv.vitam.query.parser.ParserTokens.ACTIONFILTER;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;

public class InsertTest {

	@Test
	public void testSetMult() {
		Insert insert = new Insert();
		assertTrue(insert.getFilter().size() == 0);
		insert.setMult(true);
		assertTrue(insert.getFilter().size() == 1);
		insert.setMult(false);
		assertTrue(insert.getFilter().size() == 1);
		assertTrue(insert.filter.has(ACTIONFILTER.mult.exactToken()));
		insert.resetFilter();
		assertTrue(insert.getFilter().size() == 0);
	}

	@Test
	public void testAddData() {
		Insert insert = new Insert();
		assertNull(insert.data);
		insert.addData(JsonHandler.createObjectNode().put("var1", 1));
		insert.addData(JsonHandler.createObjectNode().put("var2", "val"));
		assertEquals(2, insert.data.size());
	}

	@Test
	public void testAddRequests() {
		Insert insert = new Insert();
		assertNull(insert.requests);
		try {
			insert.addRequests(new BooleanRequest(REQUEST.and).addToBooleanRequest(new ExistsRequest(REQUEST.exists, "varA")).setRelativeDepthLimit(5));
			insert.addRequests(new PathRequest("path1", "path2"),new ExistsRequest(REQUEST.exists, "varB").setExactDepthLimit(10));
			insert.addRequests(new PathRequest("path3"));
			assertEquals(4, insert.requests.size());
		} catch (InvalidCreateOperationException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testGetFinalInsert() {
		Insert insert = new Insert();
		assertNull(insert.requests);
		try {
			insert.addRequests(new PathRequest("path3"));
			assertEquals(1, insert.requests.size());
			insert.setMult(true);
			insert.addData(JsonHandler.createObjectNode().put("var1", 1));
			ObjectNode node = insert.getFinalInsert();
			assertEquals(3, node.size());
		} catch (InvalidCreateOperationException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
