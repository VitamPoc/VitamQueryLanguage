package fr.gouv.vitam.query.construct;

import static org.junit.Assert.*;

import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.construct.action.AddAction;
import fr.gouv.vitam.query.construct.action.IncAction;
import fr.gouv.vitam.query.construct.action.PopAction;
import fr.gouv.vitam.query.construct.action.PullAction;
import fr.gouv.vitam.query.construct.action.PushAction;
import fr.gouv.vitam.query.construct.action.RenameAction;
import fr.gouv.vitam.query.construct.action.SetAction;
import fr.gouv.vitam.query.construct.action.UnsetAction;
import fr.gouv.vitam.query.construct.request.BooleanRequest;
import fr.gouv.vitam.query.construct.request.ExistsRequest;
import fr.gouv.vitam.query.construct.request.PathRequest;
import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.ACTIONFILTER;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;

public class UpdateTest {

	@Test
	public void testSetMult() {
		Update update = new Update();
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
	public void testAddActions() {
		Update update = new Update();
		assertNull(update.actions);
		try {
			update.addActions(new AddAction("varname", 1).addAddAction(true));
			update.addActions(new IncAction("varname2", 2));
			update.addActions(new PopAction("varname3", true).addPopAction("val"));
			update.addActions(new PullAction("varname4"));
			update.addActions(new PushAction("varname5", "val").addPushAction(1.0));
			update.addActions(new RenameAction("varname6", "varname7"));
			update.addActions(new SetAction("varname8", "val").addSetAction("varname9", 1));
			update.addActions(new UnsetAction("varname10", "varname11").addUnSetAction("varname12"));
			assertEquals(8, update.actions.size());
		} catch (InvalidCreateOperationException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testAddRequests() {
		Update update = new Update();
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
	public void testGetFinalUpdate() {
		Update update = new Update();
		assertNull(update.requests);
		try {
			update.addRequests(new PathRequest("path3"));
			assertEquals(1, update.requests.size());
			update.setMult(true);
			update.addActions(new IncAction("mavar"));
			ObjectNode node = update.getFinalUpdate();
			assertEquals(3, node.size());
		} catch (InvalidCreateOperationException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
