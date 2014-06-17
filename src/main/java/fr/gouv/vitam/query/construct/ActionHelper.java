/**
   This file is part of Waarp Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All Waarp Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   Waarp is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Waarp .  If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.query.construct;

import java.util.Map;

import fr.gouv.vitam.query.construct.action.AddAction;
import fr.gouv.vitam.query.construct.action.IncAction;
import fr.gouv.vitam.query.construct.action.PopAction;
import fr.gouv.vitam.query.construct.action.PullAction;
import fr.gouv.vitam.query.construct.action.PushAction;
import fr.gouv.vitam.query.construct.action.RenameAction;
import fr.gouv.vitam.query.construct.action.SetAction;
import fr.gouv.vitam.query.construct.action.UnsetAction;
import fr.gouv.vitam.query.exception.InvalidCreateOperationException;

/**
 * @author "Frederic Bregier"
 *
 */
public class ActionHelper {
	private ActionHelper() {
		// empty
	}
	public static final AddAction add(String variableName, String ...value) throws InvalidCreateOperationException {
		return new AddAction(variableName, value);
	}
	public static final AddAction add(String variableName, boolean ...value) throws InvalidCreateOperationException {
		return new AddAction(variableName, value);
	}
	public static final AddAction add(String variableName, long ...value) throws InvalidCreateOperationException {
		return new AddAction(variableName, value);
	}
	public static final AddAction add(String variableName, double ...value) throws InvalidCreateOperationException {
		return new AddAction(variableName, value);
	}
	public static final IncAction inc(String variableName, long value) throws InvalidCreateOperationException {
		return new IncAction(variableName, value);
	}
	public static final IncAction inc(String variableName) throws InvalidCreateOperationException {
		return new IncAction(variableName);
	}
	public static final PopAction pop(String variableName, String ...value) throws InvalidCreateOperationException {
		return new PopAction(variableName, value);
	}
	public static final PopAction pop(String variableName, boolean ...value) throws InvalidCreateOperationException {
		return new PopAction(variableName, value);
	}
	public static final PopAction pop(String variableName, long ...value) throws InvalidCreateOperationException {
		return new PopAction(variableName, value);
	}
	public static final PopAction pop(String variableName, double ...value) throws InvalidCreateOperationException {
		return new PopAction(variableName, value);
	}
	public static final PullAction pull(String variableName, long value) throws InvalidCreateOperationException {
		return new PullAction(variableName, value);
	}
	public static final PullAction pull(String variableName) throws InvalidCreateOperationException {
		return new PullAction(variableName);
	}
	public static final PushAction push(String variableName, String ...value) throws InvalidCreateOperationException {
		return new PushAction(variableName, value);
	}
	public static final PushAction push(String variableName, boolean ...value) throws InvalidCreateOperationException {
		return new PushAction(variableName, value);
	}
	public static final PushAction push(String variableName, long ...value) throws InvalidCreateOperationException {
		return new PushAction(variableName, value);
	}
	public static final PushAction push(String variableName, double ...value) throws InvalidCreateOperationException {
		return new PushAction(variableName, value);
	}
	public static final RenameAction rename(String variableName, String value) throws InvalidCreateOperationException {
		return new RenameAction(variableName, value);
	}
	public static final SetAction set(String variableName, String value) throws InvalidCreateOperationException {
		return new SetAction(variableName, value);
	}
	public static final SetAction set(String variableName, boolean value) throws InvalidCreateOperationException {
		return new SetAction(variableName, value);
	}
	public static final SetAction set(String variableName, long value) throws InvalidCreateOperationException {
		return new SetAction(variableName, value);
	}
	public static final SetAction set(String variableName, double value) throws InvalidCreateOperationException {
		return new SetAction(variableName, value);
	}
	public static final SetAction set(Map<String, ?> map) throws InvalidCreateOperationException {
		return new SetAction(map);
	}
	public static final UnsetAction unset(String ...variableName) throws InvalidCreateOperationException {
		return new UnsetAction(variableName);
	}
}
