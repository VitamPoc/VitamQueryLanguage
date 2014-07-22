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
import java.nio.charset.Charset;

import fr.gouv.vitam.utils.logging.VitamLogger;
import fr.gouv.vitam.utils.logging.VitamLoggerFactory;

/**
 * @author "Frederic Bregier"
 *
 */
public class FileUtil {
    private static VitamLogger LOGGER = VitamLoggerFactory.getInstance(FileUtil.class);
    /**
     * UTF-8 string
     */
    public static final String UTF_8 = "UTF-8";
    /**
     * UTF-8 Charset
     */
    public static final Charset UTF8 = Charset.forName(UTF_8);
    /**
     * @param filename
     * @return the content of the file
     * @throws IOException
     */
    public static final String readFile(final String filename) throws IOException {
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
                LOGGER.error(e);
                throw e;
            }
        }

        return builder.toString();
    }
}
