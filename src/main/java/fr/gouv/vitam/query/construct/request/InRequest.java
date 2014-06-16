/**
 * This file is part of Vitam Project.
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All Vitam Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * 
 * Vitam is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with Vitam . If not, see
 * <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.query.construct.request;

import java.util.HashSet;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.gouv.vitam.query.exception.InvalidCreateOperationException;
import fr.gouv.vitam.query.parser.ParserTokens.REQUEST;

/**
 * In and Nin requests
 * @author "Frederic Bregier"
 *
 */
public class InRequest extends Request {
	protected HashSet<Boolean> booleanVals;
	protected HashSet<Long> longVals;
	protected HashSet<Double> doubleVals;
	protected HashSet<String> stringVals;
	
	/**
	 * Clean the object
	 */
	protected void clean() {
		super.clean();
		booleanVals = null;
		longVals = null;
		doubleVals = null;
		stringVals = null;
	}
	/**
	 * In Request constructor
	 * @param inRequest in, nin
	 * @param variableName
	 * @param value
	 * @throws InvalidCreateOperationException 
	 */
	public InRequest(REQUEST inRequest, String variableName, long value) throws InvalidCreateOperationException {
		super();
		switch (inRequest) {
			case in:
			case nin: {
				if (variableName == null || variableName.trim().isEmpty()) {
					throw new InvalidCreateOperationException("Request "+inRequest+" cannot be created with empty variable name");
				}
				ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
				ArrayNode array = sub.putArray(variableName.trim());
				array.add(value);
				longVals = new HashSet<Long>();
				longVals.add(value);
				currentObject = array;
				currentREQUEST = inRequest;
				setReady(true);
				break;
			}
			default:
				throw new InvalidCreateOperationException("Request "+inRequest+" is not an In Request");
		}
	}
	/**
	 * In Request constructor
	 * @param inRequest in, nin
	 * @param variableName
	 * @param value
	 * @throws InvalidCreateOperationException 
	 */
	public InRequest(REQUEST inRequest, String variableName, double value) throws InvalidCreateOperationException {
		super();
		switch (inRequest) {
			case in:
			case nin: {
				if (variableName == null || variableName.trim().isEmpty()) {
					throw new InvalidCreateOperationException("Request "+inRequest+" cannot be created with empty variable name");
				}
				ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
				ArrayNode array = sub.putArray(variableName.trim());
				array.add(value);
				doubleVals = new HashSet<Double>();
				doubleVals.add(value);
				currentObject = array;
				currentREQUEST = inRequest;
				setReady(true);
				break;
			}
			default:
				throw new InvalidCreateOperationException("Request "+inRequest+" is not an In Request");
		}
	}
	/**
	 * In Request constructor
	 * @param inRequest in, nin
	 * @param variableName 
	 * @param value 
	 * @throws InvalidCreateOperationException 
	 */
	public InRequest(REQUEST inRequest, String variableName, String value) throws InvalidCreateOperationException {
		super();
		switch (inRequest) {
			case in:
			case nin: {
				if (variableName == null || variableName.trim().isEmpty()) {
					throw new InvalidCreateOperationException("Request "+inRequest+" cannot be created with empty variable name");
				}
				ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
				ArrayNode array = sub.putArray(variableName.trim());
				array.add(value);
				stringVals = new HashSet<String>();
				stringVals.add(value);
				currentObject = array;
				currentREQUEST = inRequest;
				setReady(true);
				break;
			}
			default:
				throw new InvalidCreateOperationException("Request "+inRequest+" is not an In or Search Request");
		}
	}
	/**
	 * In Request constructor
	 * @param inRequest in, nin
	 * @param variableName
	 * @param value
	 * @throws InvalidCreateOperationException 
	 */
	public InRequest(REQUEST inRequest, String variableName, boolean value) throws InvalidCreateOperationException {
		super();
		switch (inRequest) {
			case in:
			case nin: {
				if (variableName == null || variableName.trim().isEmpty()) {
					throw new InvalidCreateOperationException("Request "+inRequest+" cannot be created with empty variable name");
				}
				ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
				ArrayNode array = sub.putArray(variableName.trim());
				array.add(value);
				booleanVals = new HashSet<Boolean>();
				booleanVals.add(value);
				currentObject = array;
				currentREQUEST = inRequest;
				setReady(true);
				break;
			}
			default:
				throw new InvalidCreateOperationException("Request "+inRequest+" is not an In Request");
		}
	}
	/**
	 * In Request constructor
	 * 
	 * @param inRequest in, nin
	 * @param variable 
	 * @param values 
	 * @throws InvalidCreateOperationException 
	 */
	public InRequest(REQUEST inRequest, String variable, String ... values) throws InvalidCreateOperationException {
		super();
		if (variable == null || variable.trim().isEmpty()) {
			throw new InvalidCreateOperationException("Request "+inRequest+" cannot be created with empty variable name");
		}
		switch (inRequest) {
			case in:
			case nin: {
				if (variable == null || variable.trim().isEmpty()) {
					throw new InvalidCreateOperationException("Request "+inRequest+" cannot be created with empty variable name");
				}
				ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
				ArrayNode array = sub.putArray(variable.trim());
				stringVals = new HashSet<String>();
				for (String value : values) {
					if (! stringVals.contains(value)) {
						array.add(value);
						stringVals.add(value);
					}
				}
				currentObject = array;
				break;
			}
			default:
				throw new InvalidCreateOperationException("Request "+inRequest+" is not an In Request");
		}
		currentREQUEST = inRequest;
		setReady(true);
	}

	/**
	 * In Request constructor
	 * 
	 * @param inRequest in, nin
	 * @param variableName
	 * @param values
	 * @throws InvalidCreateOperationException 
	 */
	public InRequest(REQUEST inRequest, String variableName, long ... values) throws InvalidCreateOperationException {
		super();
		if (inRequest != REQUEST.in && inRequest != REQUEST.nin) {
			throw new InvalidCreateOperationException("Request "+inRequest+" is not an In Request");
		}
		if (variableName == null || variableName.trim().isEmpty()) {
			throw new InvalidCreateOperationException("Request "+currentREQUEST+" cannot be created with empty variable name");
		}
		ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
		ArrayNode array = sub.putArray(variableName.trim());
		longVals = new HashSet<Long>();
		for (long value : values) {
			if (! longVals.contains(value)) {
				array.add(value);
				longVals.add(value);
			}
		}
		currentObject = array;
		currentREQUEST = inRequest;
		setReady(true);
	}
	/**
	 * In Request constructor
	 * 
	 * @param inRequest in, nin
	 * @param variableName
	 * @param values
	 * @throws InvalidCreateOperationException 
	 */
	public InRequest(REQUEST inRequest, String variableName, double ... values) throws InvalidCreateOperationException {
		super();
		if (inRequest != REQUEST.in && inRequest != REQUEST.nin) {
			throw new InvalidCreateOperationException("Request "+inRequest+" is not an In Request");
		}
		if (variableName == null || variableName.trim().isEmpty()) {
			throw new InvalidCreateOperationException("Request "+currentREQUEST+" cannot be created with empty variable name");
		}
		ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
		ArrayNode array = sub.putArray(variableName.trim());
		doubleVals = new HashSet<Double>();
		for (double value : values) {
			if (! doubleVals.contains(value)) {
				array.add(value);
				doubleVals.add(value);
			}
		}
		currentObject = array;
		currentREQUEST = inRequest;
		setReady(true);
	}
	/**
	 * In Request constructor
	 * 
	 * @param inRequest in, nin
	 * @param variableName
	 * @param values
	 * @throws InvalidCreateOperationException 
	 */
	public InRequest(REQUEST inRequest, String variableName, boolean ... values) throws InvalidCreateOperationException {
		super();
		if (inRequest != REQUEST.in && inRequest != REQUEST.nin) {
			throw new InvalidCreateOperationException("Request "+inRequest+" is not an In Request");
		}
		if (variableName == null || variableName.trim().isEmpty()) {
			throw new InvalidCreateOperationException("Request "+currentREQUEST+" cannot be created with empty variable name");
		}
		ObjectNode sub = ((ObjectNode) currentObject).putObject(inRequest.exactToken());
		ArrayNode array = sub.putArray(variableName.trim());
		booleanVals = new HashSet<Boolean>();
		for (boolean value : values) {
			if (! booleanVals.contains(value)) {
				array.add(value);
				booleanVals.add(value);
			}
		}
		currentObject = array;
		currentREQUEST = inRequest;
		setReady(true);
	}
	/**
	 * Add an In Value to an existing In Request
	 * @param inValue
	 * @return the InRequest
	 * @throws InvalidCreateOperationException 
	 */
	public final InRequest addInValue(String ...inValue) throws InvalidCreateOperationException {
		if (currentREQUEST != REQUEST.in && currentREQUEST != REQUEST.nin) {
			throw new InvalidCreateOperationException("Cannot add an InValue since this is not an In Request: "+currentREQUEST);
		}
		ArrayNode array = (ArrayNode) currentObject;
		if (stringVals == null) {
			stringVals = new HashSet<String>();
		}
		for (String val : inValue) {
			if (! stringVals.contains(val)) {
				array.add(val);
				stringVals.add(val);
			}
		}
		return this;
	}
	/**
	 * Add an In Value to an existing In Request
	 * @param inValue
	 * @return the InRequest
	 * @throws InvalidCreateOperationException 
	 */
	public final InRequest addInValue(long ...inValue) throws InvalidCreateOperationException {
		if (currentREQUEST != REQUEST.in && currentREQUEST != REQUEST.nin) {
			throw new InvalidCreateOperationException("Cannot add an InValue since this is not an In Request: "+currentREQUEST);
		}
		ArrayNode array = (ArrayNode) currentObject;
		if (longVals == null) {
			longVals = new HashSet<Long>();
		}
		for (long l : inValue) {
			if (! longVals.contains(l)) {
				array.add(l);
				longVals.add(l);
			}
		}
		return this;
	}
	/**
	 * Add an In Value to an existing In Request
	 * @param inValue
	 * @return the InRequest
	 * @throws InvalidCreateOperationException 
	 */
	public final InRequest addInValue(double ...inValue) throws InvalidCreateOperationException {
		if (currentREQUEST != REQUEST.in && currentREQUEST != REQUEST.nin) {
			throw new InvalidCreateOperationException("Cannot add an InValue since this is not an In Request: "+currentREQUEST);
		}
		ArrayNode array = (ArrayNode) currentObject;
		if (doubleVals == null) {
			doubleVals = new HashSet<Double>();
		}
		for (double d : inValue) {
			if (! doubleVals.contains(d)) {
				array.add(d);
				doubleVals.add(d);
			}
		}
		return this;
	}
	/**
	 * Add an In Value to an existing In Request
	 * @param inValue
	 * @return the InRequest
	 * @throws InvalidCreateOperationException 
	 */
	public final InRequest addInValue(boolean ...inValue) throws InvalidCreateOperationException {
		if (currentREQUEST != REQUEST.in && currentREQUEST != REQUEST.nin) {
			throw new InvalidCreateOperationException("Cannot add an InValue since this is not an In Request: "+currentREQUEST);
		}
		ArrayNode array = (ArrayNode) currentObject;
		if (booleanVals == null) {
			booleanVals = new HashSet<Boolean>();
		}
		for (boolean b : inValue) {
			if (! booleanVals.contains(b)) {
				array.add(b);
				booleanVals.add(b);
			}
		}
		return this;
	}
}
