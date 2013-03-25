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

package org.apache.ivory.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ivory.IvoryException;
import org.apache.ivory.service.IvoryService;
import org.apache.ivory.util.StartupProperties;
import org.apache.log4j.Logger;

public class AuthenticationInitializationService implements IvoryService {

    private static final Logger LOG =
            Logger.getLogger(AuthenticationInitializationService.class);

    public static final String AUTHENTICATION_TYPE = "authentication.type";
    public static final String KERBEROS_AUTH_ENABLED = "kerberos.enabled";
    public static final String KERBEROS_KEYTAB = "keytab.file";
    public static final String KERBEROS_PRINCIPAL = "kerberos.principal";

    @Override
    public String getName() {
        return "Hadoop initialization service";
    }

    @Override
    public void init() throws IvoryException {
        // todo - replace this with AUTHENTICATION_TYPE
        boolean isKerberosAuthEnabled = Boolean.getBoolean(
            StartupProperties.get().getProperty(KERBEROS_AUTH_ENABLED, "false"));
        LOG.info("Ivory Kerberos Authentication: "
                + (isKerberosAuthEnabled ? "enabled" : "disabled"));

        if (isKerberosAuthEnabled) {
            initializeKerberos();
        } else {
            Configuration ugiConf = new Configuration();
            ugiConf.set("hadoop.security.authentication", "simple");
            UserGroupInformation.setConfiguration(ugiConf);
        }

        LOG.info("Hadoop Accessor service initialized.");
    }

    private void initializeKerberos()
            throws IvoryException {
        try {
            String keytabFile = StartupProperties.get().getProperty(
                    KERBEROS_KEYTAB,
                    System.getProperty("user.home") + "/ivory.keytab").trim();
            if (keytabFile.length() == 0) {
                throw new IvoryException("Missing required configuration " +
                        "property: " + KERBEROS_KEYTAB);
            }

            String principal = StartupProperties.get().getProperty(
                    KERBEROS_PRINCIPAL, "ivory/localhost@LOCALHOST");
            if (principal.length() == 0) {
                throw new IvoryException("Missing required configuration " +
                        "property: " + KERBEROS_PRINCIPAL);
            }

            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
            LOG.info("Got Kerberos ticket, keytab: " + keytabFile
                    + ", Ivory principal principal: " + principal);
        } catch (Exception ex) {
            throw new IvoryException("Could not initialize " + getName()
                    + ": " + ex.getMessage(), ex);
        }
    }

    @Override
    public void destroy() throws IvoryException {
        LOG.info("Hadoop Accessor service destroyed.");
    }
}
