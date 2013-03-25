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

package org.apache.ivory.entity.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.cluster.util.IvoryTestBase;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.hadoop.HadoopClientFactory;
import org.apache.ivory.util.StartupProperties;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;

public class ConfigurationStoreTest extends IvoryTestBase {

  private static Logger LOG = Logger.getLogger(ConfigurationStoreTest.class);

  private ConfigurationStore store;

  @BeforeSuite
  @AfterSuite
  public void cleanup() throws IOException, IvoryException {
    store = ConfigurationStore.get();
    Path path = new Path(
        StartupProperties.get().getProperty("config.store.uri"));
    System.out.println("path = " + path);
    FileSystem fs = FileSystem.get(path.toUri(), getConf());
    fs.delete(path, true);
    LOG.info("Cleaned up " + path);
  }

    @Test
  public void testPublish() throws Exception {
    Process process = new Process();
    process.setName("hello");
    store.publish(EntityType.PROCESS, process);
    Process p = store.get(EntityType.PROCESS, "hello");
    Assert.assertEquals(p, process);
  }

  @Test
  public void testGet() throws Exception {
    Process p = store.get(EntityType.PROCESS, "notfound");
    Assert.assertNull(p);
  }

  @Test
  public void testRemove() throws Exception {
    Process process = new Process();
    process.setName("remove");
    store.publish(EntityType.PROCESS, process);
    Process p = store.get(EntityType.PROCESS, "remove");
    Assert.assertEquals(p, process);
    store.remove(EntityType.PROCESS, "remove");
    p = store.get(EntityType.PROCESS, "remove");
    Assert.assertNull(p);
  }

  @Test
  public void testSearch() throws Exception {
    //TODO
  }
}
