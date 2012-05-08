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
package org.apache.ivory.logging;

import org.testng.annotations.Test;

public class LogMoverTest {

	@Test(enabled=false)
	public void testLogMover() throws Exception {
		LogMover.main(new String[] {
				"-oozieurl http://localhost:11000/oozie/",
				"-subflowid 0000029-120503115115560-oozie-oozi-W@user-workflow",
				"-runid 0",
				"-logdir /log/",
				"-externalid agg-coord/DEFAULT/2010-01-01T01:15Z",
				"-status SUCCESS" });
	}

}