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

package org.apache.ivory.cluster.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
//import org.apache.ivory.entity.store.ConfigurationStore;
import org.testng.annotations.BeforeSuite;

public abstract class IvoryTestBase {

    public static final String TEST_IVORY_USER_PROP = "ivory.test.user";
    public static final String TEST_IVORY_PROXY_USER_PROP =
            "ivory.test.proxy.user";
    public static final String TEST_GROUP_PROP = "ivory.test.group";

    protected Configuration conf;
//    private ConfigurationStore store;

    @BeforeSuite
    protected void setUpUGI() throws Exception {
        String ivoryUser = getIvoryUser();

        conf = new Configuration();
        conf.set("dfs.block.access.token.enable", "false");
        conf.set("dfs.permissions", "true");
        conf.set("hadoop.security.authentication", "simple");

        conf.set("hadoop.proxyuser.seetharam.groups", getTestGroup());
        conf.set("hadoop.proxyuser.seetharam.hosts", "127.0.0.1");

        conf.set("hadoop.proxyuser." + ivoryUser + ".hosts", "*");
        conf.set("hadoop.proxyuser." + ivoryUser + ".groups", getTestGroup());

        UserGroupInformation.setConfiguration(conf);
        String [] userGroups = new String[] { getTestGroup() };
        UserGroupInformation realUser = UserGroupInformation
                .createUserForTesting(ivoryUser, userGroups);
        UserGroupInformation.createProxyUserForTesting(
                getIvoryProxyUser(), realUser, userGroups);
        UserGroupInformation.createUserForTesting("testuser1", userGroups);

        // CurrentUser.authenticate(ivoryUser);

        // store = ConfigurationStore.get();
    }

    protected static String getTestGroup() {
        return System.getProperty(TEST_GROUP_PROP, "testgroup");
    }

    public static String getIvoryUser() {
        return System.getProperty(TEST_IVORY_USER_PROP,
                // System.getProperty("user.name")
                "testuser");
    }

    public static String getIvoryProxyUser() {
        return System.getProperty(TEST_IVORY_PROXY_USER_PROP,
                System.getProperty("user.name"));
    }

    protected Configuration getConf() {
        return conf;
    }

/*
    public ConfigurationStore getStore() {
        return store;
    }
*/
}