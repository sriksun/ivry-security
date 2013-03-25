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

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.cluster.Interface;
import org.apache.ivory.entity.v0.cluster.Interfaces;
import org.apache.ivory.entity.v0.cluster.Interfacetype;
import org.apache.ivory.entity.v0.cluster.Location;
import org.apache.ivory.entity.v0.cluster.Locations;
import org.apache.log4j.Logger;

public class EmbeddedCluster {

    private static Logger LOG = Logger.getLogger(EmbeddedCluster.class);

    private Configuration conf;
    private MiniDFSCluster dfsCluster;
    private MiniMRCluster mrCluster;
    protected Cluster clusterEntity;

    protected EmbeddedCluster() {
        conf = new Configuration();
    }

    public Configuration getConf() {
        return conf;
    }

    public static EmbeddedCluster newCluster(final String name, final boolean withMR)
            throws Exception {
        return createClusterAsUser(name, withMR);
    }

    public static EmbeddedCluster newCluster(final String name,
                                             final boolean withMR,
                                             final String user)
            throws Exception {

        UserGroupInformation hdfsUser = UserGroupInformation.createRemoteUser(user);
        return hdfsUser.doAs(new PrivilegedExceptionAction<EmbeddedCluster>() {
            @Override
            public EmbeddedCluster run() throws Exception {
                return createClusterAsUser(name, withMR);
            }
        });
    }

    private static EmbeddedCluster createClusterAsUser(String name,
                                                       boolean withMR)
            throws IOException {

        EmbeddedCluster cluster = new EmbeddedCluster();
        File target = new File("webapp/target");
        if (!target.exists()) {
            target = new File("target");
            System.setProperty("test.build.data", "target/" + name + "/data");
        } else {
            System.setProperty("test.build.data", "webapp/target/" + name + "/data");
        }

        String ivoryUser = initConf(cluster.conf);
        initUGI(ivoryUser);

        cluster.dfsCluster = new MiniDFSCluster(cluster.conf, 1, true, null);
        String hdfsUrl = cluster.conf.get("fs.default.name");
        LOG.info("Cluster Namenode = " + hdfsUrl);
        if (withMR) {
            System.setProperty("hadoop.log.dir", "/tmp");
            System.setProperty("org.apache.hadoop.mapred.TaskTracker", "/tmp");
            cluster.conf.set("org.apache.hadoop.mapred.TaskTracker", "/tmp");
            cluster.conf.set("org.apache.hadoop.mapred.TaskTracker", "/tmp");
            cluster.conf.set("mapreduce.jobtracker.staging.root.dir", "/ivoryUser");

            Path path = new Path("/tmp/hadoop-" + ivoryUser, "mapred");
            FileSystem fs = getFileSystem(cluster.conf);
            fs.mkdirs(path);
            fs.setPermission(path, new FsPermission((short) 511));

            cluster.mrCluster = new MiniMRCluster(1, hdfsUrl, 1);
            Configuration mrConf = cluster.mrCluster.createJobConf();
            cluster.conf.set("mapred.job.tracker",
                    mrConf.get("mapred.job.tracker"));
            cluster.conf.set("mapred.job.tracker.http.address",
                    mrConf.get("mapred.job.tracker.http.address"));
            LOG.info("Cluster JobTracker = " + cluster.conf.
                    get("mapred.job.tracker"));
        }
        cluster.buildClusterObject(name);
        return cluster;
    }

    private static FileSystem getFileSystem(final Configuration conf)
            throws IOException {
        UserGroupInformation proxy =
                UserGroupInformation.createProxyUser("oozie",
                        UserGroupInformation.createRemoteUser("seetharam"));

        FileSystem fs;
        try {
            fs = proxy.doAs(new PrivilegedExceptionAction<FileSystem>() {
                @Override
                public FileSystem run() throws Exception {
                    return FileSystem.get(conf);
                }
            });
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        return fs;
    }

    public static final String TEST_IVORY_USER_PROP = "ivory.test.user";
    public static final String TEST_IVORY_PROXY_USER_PROP =
            "ivory.test.proxy.user";
    public static final String TEST_GROUP_PROP = "ivory.test.group";

    public static String initConf(Configuration conf) {
//        String user = System.getProperty("user.name");
        conf.set("hadoop.log.dir", "/tmp");
        conf.set("hadoop.proxyuser.oozie.groups", "staff");
        conf.set("hadoop.proxyuser.oozie.hosts", "127.0.0.1");

        conf.set("hadoop.proxyuser.seetharam.groups", "*");
        conf.set("hadoop.proxyuser.seetharam.hosts", "127.0.0.1");

        conf.set("hadoop.proxyuser.hdfs.groups", "*");
        conf.set("hadoop.proxyuser.hdfs.hosts", "127.0.0.1");
        conf.set("mapreduce.jobtracker.kerberos.principal", "");
        conf.set("dfs.namenode.kerberos.principal", "");

        String ivoryUser = getIvoryUser();

        conf.set("dfs.block.access.token.enable", "false");
        conf.set("dfs.permissions", "true");
        conf.set("hadoop.security.authentication", "simple");

        conf.set("hadoop.proxyuser." + ivoryUser + ".hosts", "*");
        conf.set("hadoop.proxyuser." + ivoryUser + ".groups", getTestGroup());

        return ivoryUser;
    }

    public static void initUGI(String ivoryUser) {
        System.out.println("ivoryUser = " + ivoryUser);
        String [] userGroups = new String[] { getTestGroup() };
        System.out.println("userGroups = " + userGroups);
        UserGroupInformation realUser = UserGroupInformation
                .createUserForTesting(ivoryUser, userGroups);
        System.out.println("realUser = " + realUser);
        UserGroupInformation proxyUser = UserGroupInformation
        .createProxyUserForTesting(getIvoryProxyUser(), realUser, userGroups);
        System.out.println("getIvoryProxyUser() = " + getIvoryProxyUser());
        System.out.println("proxyUser UGI = " + proxyUser);
    }

    protected void setUpUGI() throws Exception {
        String ivoryUser = getIvoryUser();

        conf = new Configuration();
        conf.set("dfs.block.access.token.enable", "false");
        conf.set("dfs.permissions", "true");
        conf.set("hadoop.security.authentication", "simple");

        conf.set("hadoop.proxyuser." + ivoryUser + ".hosts", "*");
        conf.set("hadoop.proxyuser." + ivoryUser + ".groups", getTestGroup());

        UserGroupInformation.setConfiguration(conf);
        String [] userGroups = new String[] { getTestGroup() };
        UserGroupInformation realUser = UserGroupInformation
                .createUserForTesting(ivoryUser, userGroups);
        UserGroupInformation.createProxyUserForTesting(
                getIvoryProxyUser(), realUser, userGroups);
        UserGroupInformation.createUserForTesting("testuser1", userGroups);
    }

    public static String getTestGroup() {
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

    private void buildClusterObject(String name) {
        clusterEntity = new Cluster();
        clusterEntity.setName(name);
        clusterEntity.setColo("local");
        clusterEntity.setDescription("Embeded cluster: " + name);

        Interfaces interfaces = new Interfaces();
        interfaces.getInterfaces().add(newInterface(Interfacetype.WORKFLOW,
                "http://localhost:11000/oozie", "0.1"));
        String fsUrl = conf.get("fs.default.name");
        interfaces.getInterfaces().add(newInterface(Interfacetype.READONLY, fsUrl, "0.1"));
        interfaces.getInterfaces().add(newInterface(Interfacetype.WRITE, fsUrl, "0.1"));
        interfaces.getInterfaces().add(newInterface(Interfacetype.EXECUTE,
                        conf.get("mapred.job.tracker"), "0.1"));
		interfaces
				.getInterfaces()
				.add(newInterface(
						Interfacetype.MESSAGING,
						"vm://localhost",
						"0.1"));
      clusterEntity.setInterfaces(interfaces);

        Location location = new Location();
        location.setName("staging");
        location.setPath("/workflow/staging");
        Locations locs = new Locations();
        locs.getLocations().add(location);
        location = new Location();
        location.setName("working");
        location.setPath("/workflow/work");
        locs.getLocations().add(location);
        clusterEntity.setLocations(locs);
    }

    private Interface newInterface(Interfacetype type,
                                   String endPoint, String version) {
        Interface iface = new Interface();
        iface.setType(type);
        iface.setEndpoint(endPoint);
        iface.setVersion(version);
        return iface;
    }

    public void shutdown() {
        if (mrCluster != null) mrCluster.shutdown();
        dfsCluster.shutdown();
    }

    public Cluster getCluster() {
        return clusterEntity;
    }

    public Cluster clone(String cloneName) {
        EmbeddedCluster clone = new EmbeddedCluster();
        clone.conf = this.conf;
        clone.buildClusterObject(cloneName);
        return clone.clusterEntity;
    }
}
