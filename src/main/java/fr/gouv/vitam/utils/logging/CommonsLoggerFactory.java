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


import org.apache.commons.logging.LogFactory;

/**
 * Logger factory which creates an
 * <a href="http://commons.apache.org/logging/">Apache Commons Logging</a>
 * logger.
 */
public class CommonsLoggerFactory extends VitamLoggerFactory {

    public CommonsLoggerFactory(VitamLogLevel level) {
		super(level);
		seLevelSpecific(currentLevel);
	}

    @Override
    public VitamLogger newInstance(String name) {
    	return new CommonsLogger(LogFactory.getLog(name), name);
    }

	@Override
	protected void seLevelSpecific(VitamLogLevel level) {
		//XXX FIXME does not work for Apache Commons Logger
		switch (level) {
		case TRACE:
			LogFactory.getFactory().setAttribute("java.util.logging.ConsoleHandler.level", "FINEST");
			break;
		case DEBUG:
			LogFactory.getFactory().setAttribute("java.util.logging.ConsoleHandler.level", "FINE");
			break;
		case INFO:
			LogFactory.getFactory().setAttribute("java.util.logging.ConsoleHandler.level", "INFO");
			break;
		case WARN:
			LogFactory.getFactory().setAttribute("java.util.logging.ConsoleHandler.level", "WARNING");
			break;
		case ERROR:
			LogFactory.getFactory().setAttribute("java.util.logging.ConsoleHandler.level", "SEVERE");
			break;
		default:
			LogFactory.getFactory().setAttribute("java.util.logging.ConsoleHandler.level", "WARNING");
			break;
		}
	}
}
