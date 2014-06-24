/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package fr.gouv.vitam.utils.logging;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

public class VitamLoggerFactoryTest {
    private static final Exception e = new Exception();
    private VitamLoggerFactory oldLoggerFactory;
    private VitamLogger mock;

    @Before
    public void init() {
        oldLoggerFactory = VitamLoggerFactory.getDefaultFactory();
        VitamLoggerFactory mockFactory = createMock(VitamLoggerFactory.class);
        mock = createStrictMock(VitamLogger.class);
        expect(mockFactory.newInstance("mock")).andReturn(mock).anyTimes();
        replay(mockFactory);
        VitamLoggerFactory.setDefaultFactory(mockFactory);
    }

    @After
    public void destroy() {
        reset(mock);
        VitamLoggerFactory.setDefaultFactory(oldLoggerFactory);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullDefaultFactory() {
    	VitamLoggerFactory.setDefaultFactory(null);
    }

    @Test
    public void shouldGetInstance() {
    	VitamLoggerFactory.setDefaultFactory(oldLoggerFactory);

        String helloWorld = "Hello, world!";

        VitamLogger one = VitamLoggerFactory.getInstance("helloWorld");
        VitamLogger two = VitamLoggerFactory.getInstance(helloWorld.getClass());

        assertNotNull(one);
        assertNotNull(two);
        assertNotSame(one, two);
    }

    @Test
    public void testIsTraceEnabled() {
        expect(mock.isTraceEnabled()).andReturn(true);
        replay(mock);

        VitamLogger logger = VitamLoggerFactory.getInstance("mock");
        assertTrue(logger.isTraceEnabled());
        verify(mock);
    }

    @Test
    public void testIsDebugEnabled() {
        expect(mock.isDebugEnabled()).andReturn(true);
        replay(mock);

        VitamLogger logger = VitamLoggerFactory.getInstance("mock");
        assertTrue(logger.isDebugEnabled());
        verify(mock);
    }

    @Test
    public void testIsInfoEnabled() {
        expect(mock.isInfoEnabled()).andReturn(true);
        replay(mock);

        VitamLogger logger = VitamLoggerFactory.getInstance("mock");
        assertTrue(logger.isInfoEnabled());
        verify(mock);
    }

    @Test
    public void testIsWarnEnabled() {
        expect(mock.isWarnEnabled()).andReturn(true);
        replay(mock);

        VitamLogger logger = VitamLoggerFactory.getInstance("mock");
        assertTrue(logger.isWarnEnabled());
        verify(mock);
    }

    @Test
    public void testIsErrorEnabled() {
        expect(mock.isErrorEnabled()).andReturn(true);
        replay(mock);

        VitamLogger logger = VitamLoggerFactory.getInstance("mock");
        assertTrue(logger.isErrorEnabled());
        verify(mock);
    }

    @Test
    public void testTrace() {
        mock.trace("a");
        replay(mock);

        VitamLogger logger = VitamLoggerFactory.getInstance("mock");
        logger.trace("a");
        verify(mock);
    }

    @Test
    public void testTraceWithException() {
        mock.trace("a", e);
        replay(mock);

        VitamLogger logger = VitamLoggerFactory.getInstance("mock");
        logger.trace("a", e);
        verify(mock);
    }

    @Test
    public void testDebug() {
        mock.debug("a");
        replay(mock);

        VitamLogger logger = VitamLoggerFactory.getInstance("mock");
        logger.debug("a");
        verify(mock);
    }

    @Test
    public void testDebugWithException() {
        mock.debug("a", e);
        replay(mock);

        VitamLogger logger = VitamLoggerFactory.getInstance("mock");
        logger.debug("a", e);
        verify(mock);
    }

    @Test
    public void testInfo() {
        mock.info("a");
        replay(mock);

        VitamLogger logger = VitamLoggerFactory.getInstance("mock");
        logger.info("a");
        verify(mock);
    }

    @Test
    public void testInfoWithException() {
        mock.info("a", e);
        replay(mock);

        VitamLogger logger = VitamLoggerFactory.getInstance("mock");
        logger.info("a", e);
        verify(mock);
    }

    @Test
    public void testWarn() {
        mock.warn("a");
        replay(mock);

        VitamLogger logger = VitamLoggerFactory.getInstance("mock");
        logger.warn("a");
        verify(mock);
    }

    @Test
    public void testWarnWithException() {
        mock.warn("a", e);
        replay(mock);

        VitamLogger logger = VitamLoggerFactory.getInstance("mock");
        logger.warn("a", e);
        verify(mock);
    }

    @Test
    public void testError() {
        mock.error("a");
        replay(mock);

        VitamLogger logger = VitamLoggerFactory.getInstance("mock");
        logger.error("a");
        verify(mock);
    }

    @Test
    public void testErrorWithException() {
        mock.error("a", e);
        replay(mock);

        VitamLogger logger = VitamLoggerFactory.getInstance("mock");
        logger.error("a", e);
        verify(mock);
    }
}
