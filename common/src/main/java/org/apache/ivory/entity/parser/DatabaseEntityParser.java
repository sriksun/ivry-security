/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ivory.entity.parser;

import org.apache.ivory.entity.DatabaseHelper;
import org.apache.ivory.entity.store.StoreAccessException;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.database.Database;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseEntityParser extends EntityParser<Database> {

    private static final Logger LOG = Logger.getLogger(DatabaseEntityParser.class);

    public DatabaseEntityParser() {
        super(EntityType.DATABASE);
    }

    @Override
	public void validate(Database database)
            throws StoreAccessException, ValidationException {
        Connection connection = null;
        try {
            String driver = database.getDriver();
            LOG.info("Loading the database driver: " + driver);
            Class.forName(driver).newInstance();

            String url = DatabaseHelper.getReadOnlyEndPoint(database);
            String userName = DatabaseHelper.getPropertyValue(database, "username", "root");
            // todo: handle credentials
            String password = DatabaseHelper.getPropertyValue(database, "password", "");
            LOG.info("Creating a database connection for : " + url);
            connection = DriverManager.getConnection(url, userName, password);
            LOG.info("Created database connection: " + connection);
        } catch (Exception e) {
            throw new ValidationException("Invalid Database server configured:"
                    + DatabaseHelper.getReadOnlyEndPoint(database)
                    + ". Please drop the connector jar in shared-lib dir.", e);
        }
        finally {
            closeQuietly(connection);
        }
    }

    private void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignore) {}
        }
    }
}
