/**
   This file is part of POC MongoDB ElasticSearch Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   POC MongoDB ElasticSearch is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with POC MongoDB ElasticSearch .  If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.mdbes;

import fr.gouv.vitam.mdbes.ParserBench.FIELD;

/**
 * Use to implement variability of Field
 * @author "Frederic Bregier"
 *
 */
public class TypeField {
	public String name;
	/**
	 * Between liste, listeorder, serie
	 */
	public FIELD type;
	/**
	 * Used in liste, listeorder
	 */
	public String [] listeValeurs;
	/**
	 * Used in serie for fixed value in constant
	 */
	public String prefix;
	public String idcpt;
	public int modulo;
	/**
	 * Interval
	 */
	public int low;
	/**
	 * Interval
	 */
	public int high;
	/**
	 * saved as new current value for name
	 */
	public String saveName;
	/**
	 * add several prefix to value
	 */
	public String [] subprefixes;
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Field: ");
		builder.append(name);
		builder.append(" Type: "+type);
		builder.append(" Prefix: "+prefix);
		builder.append(" Cpt: "+idcpt);
		builder.append(" Modulo: "+modulo);
		builder.append(" Low: "+low);
		builder.append(" High: "+high);
		builder.append(" Save: "+saveName);
		if (subprefixes != null) {
			builder.append(" SubPrefix: [ ");
			for (String curname : subprefixes) {
				builder.append(curname);
				builder.append(' ');
			}
			builder.append(']');
		}
		if (listeValeurs != null) {
			builder.append(" Vals: "+listeValeurs.length);
		}
		return builder.toString();
	}
}