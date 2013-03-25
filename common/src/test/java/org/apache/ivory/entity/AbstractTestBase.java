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

import java.io.StringWriter;
import java.util.Collection;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.ivory.IvoryException;
import org.apache.ivory.cluster.util.EmbeddedCluster;
import org.apache.ivory.cluster.util.IvoryTestBase;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.cluster.Interface;
import org.apache.ivory.entity.v0.cluster.Interfacetype;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.hadoop.HadoopClientFactory;
import org.apache.ivory.util.StartupProperties;
import org.testng.annotations.BeforeClass;

public class AbstractTestBase extends IvoryTestBase {
    protected static final String PROCESS_XML = "/config/process/process-0.1.xml";
    protected static final String FEED_XML = "/config/feed/feed-0.1.xml";
    protected static final String CLUSTER_XML = "/config/cluster/cluster-0.1.xml";
    protected MiniDFSCluster dfsCluster;
    protected EmbeddedCluster embeddedCluster;

    @BeforeClass
    public void initConfigStore() throws Exception {
        cleanupStore();
        String listeners = StartupProperties.get().getProperty("configstore.listeners");
        StartupProperties.get().setProperty("configstore.listeners", 
                listeners.replace("org.apache.ivory.service.SharedLibraryHostingService", ""));
        ConfigurationStore.get().init();
    }
    
    protected void cleanupStore() throws IvoryException {
        ConfigurationStore store = ConfigurationStore.get();
        for(EntityType type:EntityType.values()) {
            Collection<String> entities = store.getEntities(type);
            for(String entity:entities)
                store.remove(type, entity);
        }
    }

    protected void storeEntity(EntityType type, String name) throws Exception {
        Unmarshaller unmarshaller = type.getUnmarshaller();
        ConfigurationStore store = ConfigurationStore.get();
        store.remove(type, name);
		switch (type) {
		case CLUSTER:
                Cluster cluster = (Cluster) unmarshaller.unmarshal(this.getClass().getResource(CLUSTER_XML));
                cluster.setName(name);
                ClusterHelper.getInterface(cluster, Interfacetype.WRITE)
                        .setEndpoint(getConf().get("fs.default.name"));
                store.publish(type, cluster);
                break;

            case FEED:
                Feed feed = (Feed) unmarshaller.unmarshal(this.getClass().getResource(FEED_XML));
                feed.setName(name);
                store.publish(type, feed);
                break;

            case PROCESS:
                Process process = (Process) unmarshaller.unmarshal(this.getClass().getResource(PROCESS_XML));
                process.setName(name);
                FileSystem fs = dfsCluster.getFileSystem();
                /*
                FileSystem fs = HadoopClientFactory.get().createFileSystem(
                        embeddedCluster.getConf());
                */
                fs.mkdirs(new Path(process.getWorkflow().getPath()));
                if (!fs.exists(new Path(process.getWorkflow()+"/lib"))) {
                	fs.mkdirs(new Path(process.getWorkflow()+"/lib"));
                }
                store.publish(type, process);
                break;
        }
    }

    public void setup() throws Exception {
		ConfigurationStore store = ConfigurationStore.get();
		for (EntityType type : EntityType.values()) {
			for (String name : store.getEntities(type)) {
				store.remove(type, name);
			}
		}
        storeEntity(EntityType.CLUSTER, "corp");
        storeEntity(EntityType.FEED, "clicks");
        storeEntity(EntityType.FEED, "impressions");
        storeEntity(EntityType.FEED, "clicksummary");
        storeEntity(EntityType.PROCESS, "clicksummary");
    }

	public String marshallEntity(final Entity entity) throws IvoryException,
			JAXBException {
		Marshaller marshaller = entity.getEntityType().getMarshaller();
		StringWriter stringWriter = new StringWriter();
		marshaller.marshal(entity, stringWriter);
		return stringWriter.toString();
	}
	
	private Interface newInterface(Interfacetype type, String endPoint,
			String version) {
		Interface iface = new Interface();
		iface.setType(type);
		iface.setEndpoint(endPoint);
		iface.setVersion(version);
		return iface;
	}
}
