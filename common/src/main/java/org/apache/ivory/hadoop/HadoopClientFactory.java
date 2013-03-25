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

package org.apache.ivory.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ivory.IvoryException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class HadoopClientFactory {
    private static final HadoopClientFactory instance = new HadoopClientFactory();

    private ConcurrentMap<String, UserGroupInformation> userUgiMap;

    private HadoopClientFactory() {
        userUgiMap = new ConcurrentHashMap<String, UserGroupInformation>();
    }

    public static HadoopClientFactory get() {
        return instance;
    }

    public FileSystem createFileSystem(final Configuration conf)
            throws IvoryException {
        try {
            return createFileSystem(new URI(conf.get("fs.default.name")), conf);
        } catch (URISyntaxException e) {
            throw new IvoryException(e);
        }
    }

    public FileSystem createFileSystem(final URI uri, final Configuration conf)
            throws IvoryException {
        try {
//        String currentUser = CurrentUser.getUser();
            String currentUser = UserGroupInformation.getCurrentUser().getUserName();
            System.out.println("currentUser = " + currentUser);
            String loginUser = UserGroupInformation.getLoginUser().getUserName();
            System.out.println("loginUser = " + loginUser);

            return createFileSystem(currentUser, uri, conf);
        } catch (IOException e) {
            throw new IvoryException("Exception while getting current user", e);
        }
    }

    /**
     * Return a FileSystem created with the provided user for the specified URI.
     *
     * @param uri  file system URI.
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return FileSystem created with the provided user/group.
     * @throws org.apache.ivory.IvoryException
     *          if the filesystem could not be created.
     */
    public FileSystem createFileSystem(String user, final URI uri,
                                       final Configuration conf)
            throws IvoryException {
        notEmpty(user, "user");

        String nameNode = uri.getAuthority();
        if (nameNode == null) {
            nameNode = conf.get("fs.default.name");
            if (nameNode != null) {
                try {
                    new URI(nameNode).getAuthority();
                } catch (URISyntaxException ex) {
                    throw new IvoryException("An exception occurred.", ex);
                }
            }
        }

        try {
            UserGroupInformation ugi = getUGI(user);
            return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                public FileSystem run() throws Exception {
                    return FileSystem.get(uri, conf);
                }
            });
        } catch (InterruptedException ex) {
            throw new IvoryException("An exception occurred:" + ex.getMessage(), ex);
        } catch (IOException ex) {
            throw new IvoryException("An exception occurred:" + ex.getMessage(), ex);
        }
    }

    /**
     * Check that a string is not null and not empty.
     * If null or emtpy throws an IllegalArgumentException.
     *
     * @param value value.
     * @param name  parameter name for the exception message.
     * @return the given value.
     */
    public static String notEmpty(String value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }

        if (value.length() == 0) {
            throw new IllegalArgumentException(name + " cannot be empty");
        }

        return value;
    }

    private UserGroupInformation getUGI(String user) throws IOException {
        UserGroupInformation ugi = userUgiMap.get(user);
        if (ugi == null) {
            // taking care of a race condition, the latest UGI will be discarded
            ugi = UserGroupInformation.createProxyUser(user,
                    UserGroupInformation.getLoginUser());
            userUgiMap.putIfAbsent(user, ugi);
        }

        return ugi;
    }
}
