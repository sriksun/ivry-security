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

package org.apache.ivory.entity;

import org.apache.ivory.entity.v0.database.Interface;
import org.apache.ivory.entity.v0.database.Interfacetype;
import org.apache.ivory.entity.v0.feed.Database;
import org.apache.ivory.entity.v0.feed.Property;

public final class DatabaseHelper {

    private DatabaseHelper() {
    }

    public static String getReadOnlyEndPoint(
            org.apache.ivory.entity.v0.database.Database database) {
        String endpoint = null;
        for (Interface anInterface : database.getInterfaces().getInterfaces()) {
            if (anInterface.getType() == Interfacetype.READONLY) {
                endpoint = anInterface.getEndpoint();
                break;
            }
        }

        return endpoint;
    }

    public static String getPropertyValue(Database feedDatabase,
                                          String propName, String defaultValue) {
        if (feedDatabase.getProperties() != null) {
            for (Property prop : feedDatabase.getProperties().getProperties()) {
                if (prop.getName().equals(propName))
                    return prop.getValue();
            }
        }

        return defaultValue;
    }

    public static String getPropertyValue(
            org.apache.ivory.entity.v0.database.Database databaseEntity,
            String propName, String defaultValue) {
        if (databaseEntity.getProperties() != null) {
            for (org.apache.ivory.entity.v0.database.Property prop :
                    databaseEntity.getProperties().getProperties()) {
                if (prop.getName().equals(propName))
                    return prop.getValue();
            }
        }

        return defaultValue;
    }
}
