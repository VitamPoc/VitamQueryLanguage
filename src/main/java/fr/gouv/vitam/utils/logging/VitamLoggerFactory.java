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

/**
 * Creates an {@link VitamLogger} or changes the default factory
 * implementation.  This factory allows you to choose what logging framework
 * VITAM should use.  The default factory is {@link Slf4JLoggerFactory}.  If SLF4J
 * is not available, {@link Log4JLoggerFactory} is used.  If Log4J is not available,
 * {@link JdkLoggerFactory} is used.  You can change it to your preferred
 * logging framework before other VITAM classes are loaded:
 * <pre>
 * {@link VitamLoggerFactory}.setDefaultFactory(new {@link Log4JLoggerFactory}());
 * </pre>
 * Please note that the new default factory is effective only for the classes
 * which were loaded after the default factory is changed.  Therefore,
 * {@link #setDefaultFactory(VitamLoggerFactory)} should be called as early
 * as possible and shouldn't be called more than once.
 */
public abstract class VitamLoggerFactory {
    private static volatile VitamLoggerFactory defaultFactory;
    protected static VitamLogLevel currentLevel = null;

    static {
        final String name = VitamLoggerFactory.class.getName();
        VitamLoggerFactory f;
        try {
            f = new LogbackLoggerFactory(true);
            f.newInstance(name).debug("Using Logback (SLF4J) as the default logging framework");
            defaultFactory = f;
        } catch (Throwable t1) {
            try {
                f = new Log4JLoggerFactory(null);
                f.newInstance(name).debug("Using Log4J as the default logging framework", t1);
            } catch (Throwable t2) {
                f = new JdkLoggerFactory(null);
                f.newInstance(name).debug("Using java.util.logging as the default logging framework", t2);
            }
        }

        defaultFactory = f;
    }

    /**
     * Returns the default factory.  The initial default factory is
     * {@link JdkLoggerFactory}.
     */
    public static VitamLoggerFactory getDefaultFactory() {
        return defaultFactory;
    }

    /**
     * Changes the default factory.
     */
    public static void setDefaultFactory(VitamLoggerFactory defaultFactory) {
        if (defaultFactory == null) {
            throw new NullPointerException("defaultFactory");
        }
        VitamLoggerFactory.defaultFactory = defaultFactory;
    }

    /**
     * Creates a new logger instance with the name of the specified class.
     */
    public static VitamLogger getInstance(Class<?> clazz) {
        return getInstance(clazz.getName());
    }

    /**
     * Creates a new logger instance with the specified name.
     */
    public static VitamLogger getInstance(String name) {
        return getDefaultFactory().newInstance(name);
    }

    public static void setLogLevel(VitamLogLevel level) {
        setInternalLogLevel(level);
        if (currentLevel != null) {
            getDefaultFactory().seLevelSpecific(currentLevel);
        }
    }

    protected static synchronized void setInternalLogLevel(VitamLogLevel level) {
        if (level != null) {
            currentLevel = level;
        }
    }
    
    public VitamLoggerFactory(VitamLogLevel level) {
        setInternalLogLevel(level);
        if (currentLevel == null) {
            setInternalLogLevel(getLevelSpecific());
        }
    }

    protected abstract VitamLogLevel getLevelSpecific();
    protected abstract void seLevelSpecific(VitamLogLevel level);
    
    /**
     * Creates a new logger instance with the specified name.
     */
    protected abstract VitamLogger newInstance(String name);
}
