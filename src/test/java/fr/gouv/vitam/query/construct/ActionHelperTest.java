package fr.gouv.vitam.query.construct;

import static org.junit.Assert.*;

import java.util.HashMap;

import org.junit.Test;

import fr.gouv.vitam.query.construct.action.Action;
import fr.gouv.vitam.query.construct.action.AddAction;
import fr.gouv.vitam.query.construct.action.PopAction;
import fr.gouv.vitam.query.construct.action.PushAction;
import fr.gouv.vitam.query.construct.action.SetAction;
import fr.gouv.vitam.query.construct.action.UnsetAction;
import fr.gouv.vitam.query.exception.InvalidCreateOperationException;

public class ActionHelperTest {

    @Test
    public void testAdd() {
        try {
            Action action = ActionHelper.add("var1", "val1", "val2");
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 2);
            action = ActionHelper.add("var1", 1, 2);
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 2);
            action = ActionHelper.add("var1", 1.0, 2.0);
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 2);
            action = ActionHelper.add("var1", true, false);
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 2);
            ((AddAction) action).addAddAction(true, false, true);
            assertTrue(action.getCurrentObject().size() == 5);
            ((AddAction) action).addAddAction(3, 4, 5);
            assertTrue(action.getCurrentObject().size() == 8);
            ((AddAction) action).addAddAction(3.0, 4.0, 5.0);
            assertTrue(action.getCurrentObject().size() == 11);
            ((AddAction) action).addAddAction("val1", "val2", "val3");
            assertTrue(action.getCurrentObject().size() == 14);
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testInc() {
        try {
            Action action = ActionHelper.inc("var1");
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 1);
            assertTrue(action.getCurrentObject().path("var1").asInt() == 1);
            action = ActionHelper.inc("var1", 5);
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 1);
            assertTrue(action.getCurrentObject().path("var1").asInt() == 5);
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPop() {
        try {
            Action action = ActionHelper.pop("var1", "val1", "val2");
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 2);
            action = ActionHelper.pop("var1", 1, 2);
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 2);
            action = ActionHelper.pop("var1", 1.0, 2.0);
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 2);
            action = ActionHelper.pop("var1", true, false);
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 2);
            ((PopAction) action).addPopAction(true, false, true);
            assertTrue(action.getCurrentObject().size() == 5);
            ((PopAction) action).addPopAction(3, 4, 5);
            assertTrue(action.getCurrentObject().size() == 8);
            ((PopAction) action).addPopAction(3.0, 4.0, 5.0);
            assertTrue(action.getCurrentObject().size() == 11);
            ((PopAction) action).addPopAction("val1", "val2", "val3");
            assertTrue(action.getCurrentObject().size() == 14);
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPull() {
        try {
            Action action = ActionHelper.pull("var1");
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 1);
            assertTrue(action.getCurrentObject().path("var1").asInt() == 1);
            action = ActionHelper.pull("var1", 2);
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 1);
            assertTrue(action.getCurrentObject().path("var1").asInt() == 2);
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPush() {
        try {
            Action action = ActionHelper.push("var1", "val1", "val2");
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 2);
            action = ActionHelper.push("var1", 1, 2);
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 2);
            action = ActionHelper.push("var1", 1.0, 2.0);
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 2);
            action = ActionHelper.push("var1", true, false);
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 2);
            ((PushAction) action).addPushAction(true, false, true);
            assertTrue(action.getCurrentObject().size() == 5);
            ((PushAction) action).addPushAction(3, 4, 5);
            assertTrue(action.getCurrentObject().size() == 8);
            ((PushAction) action).addPushAction(3.0, 4.0, 5.0);
            assertTrue(action.getCurrentObject().size() == 11);
            ((PushAction) action).addPushAction("val1", "val2", "val3");
            assertTrue(action.getCurrentObject().size() == 14);
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testRename() {
        try {
            Action action = ActionHelper.rename("var1", "var2");
            assertTrue(action.getCurrentAction().size() == 1);
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testSet() {
        try {
            Action action = ActionHelper.set("var1", "val1");
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 1);
            HashMap<String, Integer> map = new HashMap<>();
            map.put("a", 1);
            map.put("b", 2);
            map.put("c", 3);
            action = ActionHelper.set(map);
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 3);
            action = ActionHelper.set("var1", 1);
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 1);
            action = ActionHelper.set("var1", 1.0);
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 1);
            action = ActionHelper.set("var1", true);
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 1);
            ((SetAction) action).addSetAction("var2", false);
            assertTrue(action.getCurrentObject().size() == 2);
            ((SetAction) action).addSetAction("var3",  5);
            assertTrue(action.getCurrentObject().size() == 3);
            ((SetAction) action).addSetAction("var4", 5.0);
            assertTrue(action.getCurrentObject().size() == 4);
            ((SetAction) action).addSetAction("var5", "val3");
            assertTrue(action.getCurrentObject().size() == 5);
            ((SetAction) action).addSetAction("var2", false);
            assertTrue(action.getCurrentObject().size() == 5);
            ((SetAction) action).addSetAction("var3",  5);
            assertTrue(action.getCurrentObject().size() == 5);
            ((SetAction) action).addSetAction("var4", 5.0);
            assertTrue(action.getCurrentObject().size() == 5);
            ((SetAction) action).addSetAction("var5", "val3");
            assertTrue(action.getCurrentObject().size() == 5);
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testUnset() {
        try {
            Action action = ActionHelper.unset("var1");
            assertTrue(action.getCurrentAction().size() == 1);
            assertTrue(action.getCurrentObject().size() == 1);
            ((UnsetAction) action).addUnSetAction("var5", "var3");
            assertTrue(action.getCurrentObject().size() == 3);
        } catch (InvalidCreateOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

}
