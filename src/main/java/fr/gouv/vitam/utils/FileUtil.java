/**
 * This file is part of POC MongoDB ElasticSearch Project.
 *
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * All POC MongoDB ElasticSearch Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published
 * by the Free Software Foundation, either versionRank 3 of the License, or
 * (at your option) any later versionRank.
 *
 * POC MongoDB ElasticSearch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with POC MongoDB ElasticSearch . If not, see <http://www.gnu.org/licenses/>.
 */
package fr.gouv.vitam.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author "Frederic Bregier"
 *
 */
public class FileUtil {

    /**
     * @param filename
     * @return the content of the file
     * @throws IOException
     */
    public static String readFile(final String filename) throws IOException {
        final StringBuilder builder = new StringBuilder();

        final File file = new File(filename);
        if (file.canRead()) {
            try {
                final FileInputStream inputStream = new FileInputStream(file);
                final InputStreamReader reader = new InputStreamReader(inputStream);
                final BufferedReader buffered = new BufferedReader(reader);
                String line;
                while ((line = buffered.readLine()) != null) {
                    builder.append(line);
                }
                buffered.close();
                reader.close();
                inputStream.close();
            } catch (final IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                throw e;
            }
        }

        return builder.toString();
    }
}
