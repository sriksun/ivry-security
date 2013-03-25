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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.hadoop.HadoopClientFactory;
import org.apache.log4j.Logger;

public class OozieHouseKeepingService implements WorkflowEngineActionListener {

    private static Logger LOG = Logger.getLogger(OozieHouseKeepingService.class);

    @Override
    public void beforeSchedule(Entity entity, String cluster) throws IvoryException {
    }

    @Override
    public void afterSchedule(Entity entity, String cluster) throws IvoryException {
    }

    @Override
    public void beforeDelete(Entity entity, String cluster) throws IvoryException {
    }

    @Override
    public void afterDelete(Entity entity, String clusterName) throws IvoryException {
        try {
            Cluster cluster = EntityUtil.getEntity(EntityType.CLUSTER, clusterName);
            Path entityPath = new Path(ClusterHelper.getLocation(cluster, "staging"), EntityUtil.getStagingPath(entity)).getParent();
            LOG.info("Deleting entity path " + entityPath + " on cluster " + clusterName);
            
            Configuration conf = ClusterHelper.getConfiguration(cluster);
            FileSystem fs = HadoopClientFactory.get().createFileSystem(conf);
            if (fs.exists(entityPath) && !fs.delete(entityPath, true)) {
                throw new IvoryException("Unable to cleanup entity path: " + entityPath);
            }
        } catch (Exception e) {
            throw new IvoryException("Failed to cleanup entity path for " + entity.toShortString() + " on cluster " + clusterName, e);
        }
    }

    @Override
    public void beforeSuspend(Entity entity, String cluster) throws IvoryException {
    }

    @Override
    public void afterSuspend(Entity entity, String cluster) throws IvoryException {
    }

    @Override
    public void beforeResume(Entity entity, String cluster) throws IvoryException {
    }

    @Override
    public void afterResume(Entity entity, String cluster) throws IvoryException {
    }
}
