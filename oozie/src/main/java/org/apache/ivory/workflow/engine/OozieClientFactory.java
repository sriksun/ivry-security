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

package org.apache.ivory.workflow.engine;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CustomOozieClient;
import org.apache.oozie.client.OozieClient;
//import org.apache.oozie.local.LocalOozie;

import java.util.concurrent.ConcurrentHashMap;

public class OozieClientFactory {

	private static final Logger LOG =
            Logger.getLogger(OozieClientFactory.class);

    private static final ConcurrentHashMap<String, OozieClient> cache =
            new ConcurrentHashMap<String, OozieClient>();
    private static final String LOCAL_OOZIE = "local";
    private static volatile boolean localInitialized = false;

    public synchronized static OozieClient get(Cluster cluster)
            throws IvoryException {
        assert cluster != null : "Cluster cant be null";
        String oozieUrl = ClusterHelper.getOozieUrl(cluster);
        return getClient(oozieUrl);
    }

    public synchronized static OozieClient getClient(String oozieUrl)
            throws IvoryException {
        if (!cache.containsKey(oozieUrl)) {
            OozieClient ref = getClientRef(oozieUrl);
            LOG.info("Caching Oozie client object for " + oozieUrl);
            cache.putIfAbsent(oozieUrl, ref);
            return ref;
        } else {
            return cache.get(oozieUrl);
        }
    }

    public static OozieClient get(String cluster) throws IvoryException {
        return get((Cluster) ConfigurationStore.get().get(
                EntityType.CLUSTER, cluster));
    }

    private static OozieClient getClientRef(String oozieUrl)
            throws IvoryException {
        if (LOCAL_OOZIE.equals(oozieUrl)) {
            return getLocalOozieClient();
        } else {
            return new CustomOozieClient(oozieUrl);
        }
    }

    private static OozieClient getLocalOozieClient() throws IvoryException {
        try {
            if (!localInitialized) {
                //LocalOozie.start();
                localInitialized = true;
            }
            //return LocalOozie.getClient();
            return null;
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }
}
