/**
 * This file is part of Waarp Project.
 *
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * All Waarp Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Waarp is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Waarp . If not, see <http://www.gnu.org/licenses/>.
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
public final class ActionHelper {
    private ActionHelper() {
        // empty
    }

    /**
     *
     * @param variableName
     * @param value
     * @return an AddAction
     * @throws InvalidCreateOperationException
     */
    public static final AddAction add(final String variableName, final String... value) throws InvalidCreateOperationException {
        return new AddAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return an AddAction
     * @throws InvalidCreateOperationException
     */
    public static final AddAction add(final String variableName, final boolean... value) throws InvalidCreateOperationException {
        return new AddAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return an AddAction
     * @throws InvalidCreateOperationException
     */
    public static final AddAction add(final String variableName, final long... value) throws InvalidCreateOperationException {
        return new AddAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return an AddAction
     * @throws InvalidCreateOperationException
     */
    public static final AddAction add(final String variableName, final double... value) throws InvalidCreateOperationException {
        return new AddAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return an IncAction
     * @throws InvalidCreateOperationException
     */
    public static final IncAction inc(final String variableName, final long value) throws InvalidCreateOperationException {
        return new IncAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @return an IncAction using default value 1
     * @throws InvalidCreateOperationException
     */
    public static final IncAction inc(final String variableName) throws InvalidCreateOperationException {
        return new IncAction(variableName);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a PopAction
     * @throws InvalidCreateOperationException
     */
    public static final PopAction pop(final String variableName, final String... value) throws InvalidCreateOperationException {
        return new PopAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a PopAction
     * @throws InvalidCreateOperationException
     */
    public static final PopAction pop(final String variableName, final boolean... value) throws InvalidCreateOperationException {
        return new PopAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a PopAction
     * @throws InvalidCreateOperationException
     */
    public static final PopAction pop(final String variableName, final long... value) throws InvalidCreateOperationException {
        return new PopAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a PopAction
     * @throws InvalidCreateOperationException
     */
    public static final PopAction pop(final String variableName, final double... value) throws InvalidCreateOperationException {
        return new PopAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a PullAction
     * @throws InvalidCreateOperationException
     */
    public static final PullAction pull(final String variableName, final long value) throws InvalidCreateOperationException {
        return new PullAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @return a PullAction using default value 1
     * @throws InvalidCreateOperationException
     */
    public static final PullAction pull(final String variableName) throws InvalidCreateOperationException {
        return new PullAction(variableName);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a PushAction
     * @throws InvalidCreateOperationException
     */
    public static final PushAction push(final String variableName, final String... value) throws InvalidCreateOperationException {
        return new PushAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a PushAction
     * @throws InvalidCreateOperationException
     */
    public static final PushAction push(final String variableName, final boolean... value) throws InvalidCreateOperationException {
        return new PushAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a PushAction
     * @throws InvalidCreateOperationException
     */
    public static final PushAction push(final String variableName, final long... value) throws InvalidCreateOperationException {
        return new PushAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a PushAction
     * @throws InvalidCreateOperationException
     */
    public static final PushAction push(final String variableName, final double... value) throws InvalidCreateOperationException {
        return new PushAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param newName
     * @return a RenameAction
     * @throws InvalidCreateOperationException
     */
    public static final RenameAction rename(final String variableName, final String newName)
            throws InvalidCreateOperationException {
        return new RenameAction(variableName, newName);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a SetAction
     * @throws InvalidCreateOperationException
     */
    public static final SetAction set(final String variableName, final String value) throws InvalidCreateOperationException {
        return new SetAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a SetAction
     * @throws InvalidCreateOperationException
     */
    public static final SetAction set(final String variableName, final boolean value) throws InvalidCreateOperationException {
        return new SetAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a SetAction
     * @throws InvalidCreateOperationException
     */
    public static final SetAction set(final String variableName, final long value) throws InvalidCreateOperationException {
        return new SetAction(variableName, value);
    }

    /**
     *
     * @param variableName
     * @param value
     * @return a SetAction
     * @throws InvalidCreateOperationException
     */
    public static final SetAction set(final String variableName, final double value) throws InvalidCreateOperationException {
        return new SetAction(variableName, value);
    }

    /**
     *
     * @param map
     *            map of variableName for values
     * @return a SectAction
     * @throws InvalidCreateOperationException
     */
    public static final SetAction set(final Map<String, ?> map) throws InvalidCreateOperationException {
        return new SetAction(map);
    }

    /**
     *
     * @param variableName
     * @return an UnsetAction
     * @throws InvalidCreateOperationException
     */
    public static final UnsetAction unset(final String... variableName) throws InvalidCreateOperationException {
        return new UnsetAction(variableName);
    }
}
