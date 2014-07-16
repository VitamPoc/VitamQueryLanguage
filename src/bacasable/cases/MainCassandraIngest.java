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

package fr.gouv.vitam.cases;

import java.util.Iterator;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import fr.gouv.vitam.utils.UUID;

/**
 * @author "Frederic Bregier"
 *
 */
public class MainCassandraIngest {

    /**
     * @param args
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Not enough args: node");
            return;
        }
        String node = args[0];
        Cluster cluster = Cluster.builder()
                .addContactPoint(node)
                .build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", 
              metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
           System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
              host.getDatacenter(), host.getAddress(), host.getRack());
        }
        Session session = cluster.connect();
        session.execute("DROP KEYSPACE IF EXISTS VitamIdx");
        
        session.execute("CREATE KEYSPACE IF NOT EXISTS VitamIdx WITH replication "
                + "= {'class': 'SimpleStrategy', 'replication_factor':1};");
        session.execute("CREATE TABLE IF NOT EXISTS VitamIdx.DAip ("
                + "id varchar PRIMARY KEY,"
                + "nb int);");
        UUID uuid = new UUID();
        session.execute("INSERT INTO VitamIdx.DAip (id, nb)"
                + " VALUES ("
                + "'"+uuid+"', 10);");
        ResultSet results = session.execute("SELECT * FROM VitamIdx.DAip "
                + "WHERE id = '"+uuid+"';");
        for (Row row : results) {
            System.out.println(row.getString("id")+" "+row.getInt("nb"));
        }
        PreparedStatement statement = session.prepare("INSERT INTO VitamIdx.DAip (id, nb) VALUES (?,?);");
        BoundStatement boundStatement = new BoundStatement(statement);
        UUID uuid2 = new UUID();
        session.execute(boundStatement.bind(uuid2.toString(), 12));
        UUID uuid3 = new UUID();
        session.execute(boundStatement.bind(uuid3.toString(), 13));
        
        cluster.close();
    }
}
