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

package org.apache.ivory.cli;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Map;

import junit.framework.Assert;

import org.apache.ivory.resource.AbstractTestBase;
import org.testng.annotations.Test;

//Refactor both the classes to move this methods to helper;
public class IvoryCLITest extends AbstractTestBase{

	private InMemoryWriter stream = new InMemoryWriter(System.out);
	// private static final String BROKER_URL =
	// "tcp://localhost:61616?daemon=true";
	private static final boolean enableTest = true;

	@Test(enabled = enableTest)
	public void testSubmitEntityValidCommands() throws Exception {
		
		IvoryCLI.OUT_STREAM = stream;

		String filePath;
		Map<String, String> overlay = getUniqueOverlay();

		filePath = overlayParametersOverTemplate(CLUSTER_FILE_TEMPLATE, overlay);
		Assert.assertEquals(
				0,
				executeWithURL("entity -submit -type cluster -file " + filePath));
		Assert.assertEquals(stream.buffer.toString().trim(),
				"default/Submit successful (cluster) " + clusterName);

		filePath = overlayParametersOverTemplate(FEED_TEMPLATE1, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));
		Assert.assertEquals(
				stream.buffer.toString().trim(),
				"default/Submit successful (feed) "
						+ overlay.get("inputFeedName"));

		filePath = overlayParametersOverTemplate(FEED_TEMPLATE2, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));
		Assert.assertEquals(
				stream.buffer.toString().trim(),
				"default/Submit successful (feed) "
						+ overlay.get("outputFeedName"));

		filePath = overlayParametersOverTemplate(PROCESS_TEMPLATE, overlay);
		Assert.assertEquals(
				0,
				executeWithURL("entity -submit -type process -file " + filePath));
		Assert.assertEquals(
				stream.buffer.toString().trim(),
				"default/Submit successful (process) "
						+ overlay.get("processName"));
	}
	
	@Test(enabled = enableTest)
	public void testListWithEmptyConfigStore() throws Exception
	{
		Assert.assertEquals(
				0,
				executeWithURL("entity -list -type process "));
	}
	
	@Test(enabled = enableTest)
	public void testSubmitAndScheduleEntityValidCommands() throws Exception {

        Thread.sleep(5000);
		String filePath;
		Map<String, String> overlay = getUniqueOverlay();

		filePath = overlayParametersOverTemplate(CLUSTER_FILE_TEMPLATE, overlay);
		Assert.assertEquals(-1,
				executeWithURL("entity -submitAndSchedule -type cluster -file "
						+ filePath));

		filePath = overlayParametersOverTemplate(FEED_TEMPLATE1, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submitAndSchedule -type feed -file "
						+ filePath));
		filePath = overlayParametersOverTemplate(FEED_TEMPLATE2, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submitAndSchedule -type feed -file "
						+ filePath));
		filePath = overlayParametersOverTemplate(FEED_TEMPLATE1, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));

		filePath = overlayParametersOverTemplate(FEED_TEMPLATE2, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));

		filePath = overlayParametersOverTemplate(PROCESS_TEMPLATE, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submitAndSchedule -type process -file "
						+ filePath));

        Thread.sleep(5000);
	}

	@Test(enabled = enableTest)
	public void testValidateValidCommands() throws Exception {

		String filePath;
		Map<String, String> overlay = getUniqueOverlay();

		filePath = overlayParametersOverTemplate(CLUSTER_FILE_TEMPLATE, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -validate -type cluster -file "
						+ filePath));
		Assert.assertEquals(
				0,
				executeWithURL("entity -submit -type cluster -file " + filePath));

		filePath = overlayParametersOverTemplate(FEED_TEMPLATE1, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -validate -type feed -file " + filePath));
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));

		filePath = overlayParametersOverTemplate(FEED_TEMPLATE2, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -validate -type feed -file " + filePath));
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));

		filePath = overlayParametersOverTemplate(PROCESS_TEMPLATE, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -validate -type process -file "
						+ filePath));
		Assert.assertEquals(
				0,
				executeWithURL("entity -submit -type process -file " + filePath));

	}

	@Test(enabled = enableTest)
	public void testDefinitionEntityValidCommands() throws Exception {
		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(0,
				executeWithURL("entity -definition -type cluster -name "
						+ overlay.get("cluster")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -definition -type feed -name "
						+ overlay.get("inputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -definition -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(0,
				executeWithURL("entity -definition -type process -name "
						+ overlay.get("processName")));

	}

	@Test(enabled = enableTest)
	public void testScheduleEntityValidCommands() throws Exception {

		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(-1,
				executeWithURL("entity -schedule -type cluster -name "
						+ overlay.get("cluster")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -schedule -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(0,
				executeWithURL("entity -schedule -type process -name "
						+ overlay.get("processName")));

	}

	@Test(enabled = enableTest)
	public void testSuspendResumeStatusEntityValidCommands() throws Exception {

        Thread.sleep(5000);
		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(
				0,
				executeWithURL("entity -status -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -status -type process -name "
						+ overlay.get("processName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -schedule -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(0,
				executeWithURL("entity -schedule -type process -name "
						+ overlay.get("processName")));
		
		waitForProcessWFtoStart();

		Assert.assertEquals(
				0,
				executeWithURL("entity -suspend -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -suspend -type process -name "
						+ overlay.get("processName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -status -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -status -type process -name "
						+ overlay.get("processName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -resume -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -resume -type process -name "
						+ overlay.get("processName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -status -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -status -type process -name "
						+ overlay.get("processName")));

        Thread.sleep(5000);
	}

	@Test(enabled = enableTest)
	public void testSubCommandPresence() throws Exception {
		Assert.assertEquals(-1, executeWithURL("entity -type cluster "));
	}

	@Test(enabled = enableTest)
	public void testDeleteEntityValidCommands() throws Exception {

		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(
				-1,
				executeWithURL("entity -delete -type cluster -name "
						+ overlay.get("cluster")));

		Assert.assertEquals(
				-1,
				executeWithURL("entity -delete -type feed -name "
						+ overlay.get("inputFeedName")));

		Assert.assertEquals(
				-1,
				executeWithURL("entity -delete -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -delete -type process -name "
						+ overlay.get("processName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -delete -type feed -name "
						+ overlay.get("inputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -delete -type feed -name "
						+ overlay.get("outputFeedName")));

	}

	@Test(enabled = enableTest)
	public void testInvalidCLIEntitycommands() throws Exception {

		Map<String, String> overlay = getUniqueOverlay();
		overlayParametersOverTemplate(FEED_TEMPLATE1, overlay);
		Assert.assertEquals(-1,
				executeWithURL("entity -submit -type feed -name " + "name"));

		Assert.assertEquals(-1,
				executeWithURL("entity -schedule -type feed -file " + "name"));
	}

	@Test(enabled = enableTest)
	public void testInstanceRunningAndStatusCommands() throws Exception {
		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(0,
				executeWithURL("entity -schedule -type process -name "
						+ overlay.get("processName")));
		
		Assert.assertEquals(0,
				executeWithURL("entity -schedule -type feed -name "
						+ overlay.get("outputFeedName")));
		waitForProcessWFtoStart();
		
		Assert.assertEquals(0,
				executeWithURL("instance -status -type feed -name "
						+ overlay.get("outputFeedName")
						+ " -start " + START_INSTANCE));

		Assert.assertEquals(0,
				executeWithURL("instance -running -type process -name "
						+ overlay.get("processName")));

		Assert.assertEquals(0,
				executeWithURL("instance -status -type process -name "
						+ overlay.get("processName")
						+ " -start " + START_INSTANCE));
	}

	@Test(enabled = enableTest)
	public void testInstanceSuspendAndResume() throws Exception {
        Thread.sleep(5000);
		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(0,
				executeWithURL("entity -schedule -type process -name "
						+ overlay.get("processName")));


		Assert.assertEquals(0,
				executeWithURL("instance -suspend -type process -name "
						+ overlay.get("processName")
						+ " -start " + START_INSTANCE + " -end " + START_INSTANCE));
		
		Assert.assertEquals(0,
				executeWithURL("instance -resume -type process -name "
						+ overlay.get("processName")
						+ " -start " + START_INSTANCE + " -end " + START_INSTANCE));
        Thread.sleep(5000);
	}

    private static final String START_INSTANCE = "2012-04-20T00:00Z";

	@Test(enabled = enableTest)
	public void testInstanceKillAndRerun() throws Exception {
		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(0,
				executeWithURL("entity -schedule -type process -name "
						+ overlay.get("processName")));

		waitForProcessWFtoStart();
		Assert.assertEquals(
				0,
				executeWithURL("instance -kill -type process -name "
						+ overlay.get("processName")
						+ " -start " + START_INSTANCE + " -end " + START_INSTANCE));
		
		Assert.assertEquals(
				0,
				executeWithURL("instance -rerun -type process -name "
						+ overlay.get("processName")
						+ " -start " + START_INSTANCE + " -file "
						+ createTempJobPropertiesFile()));
	}
	
	@Test(enabled = enableTest)
	public void testContinue() throws Exception{
		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(0,
				executeWithURL("entity -schedule -type process -name "
						+ overlay.get("processName")));

		waitForProcessWFtoStart();
		Assert.assertEquals(
				0,
				executeWithURL("instance -kill -type process -name "
						+ overlay.get("processName")
						+ " -start " + START_INSTANCE + " -end " + START_INSTANCE));
		
		Assert.assertEquals(
				0,
				executeWithURL("instance -continue -type process -name "
						+ overlay.get("processName")
						+ " -start " + START_INSTANCE ));
	}
	
	@Test(enabled = enableTest)
	public void testInvalidCLIInstanceCommands() throws Exception {
		// no command
		Assert.assertEquals(-1, executeWithURL(" -kill -type process -name "
				+ "name" + " -start 2010-01-01T01:00Z  -end 2010-01-01T03:00Z"));

		Assert.assertEquals(-1, executeWithURL("instance -kill  " + "name"
				+ " -start 2010-01-01T01:00Z  -end 2010-01-01T01:00Z"));

		Assert.assertEquals(-1,
				executeWithURL("instance -kill -type process -name " + "name"
						+ " -end 2010-01-01T03:00Z"));

		Assert.assertEquals(-1,
				executeWithURL("instance -kill -type process -name "
						+ " -start 2010-01-01T01:00Z  -end 2010-01-01T03:00Z"));

	}

	@Test(enabled = enableTest)
	public void testIvoryURL() throws Exception {
		Assert.assertEquals(-1, new IvoryCLI()
				.run(("instance -status -type process -name " + "processName"
						+ " -start 2010-01-01T01:00Z  -end 2010-01-01T03:00Z")
						.split("\\s")));

		Assert.assertEquals(-1, new IvoryCLI()
				.run(("instance -status -type process -name "
						+ "processName -url http://unknownhost:1234/"
						+ " -start 2010-01-01T01:00Z  -end 2010-01-01T03:00Z")
						.split("\\s")));

		
	}
	
	@Test(enabled = enableTest)
	public void testClientProperties() throws Exception {
		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(
				0,
				new IvoryCLI().run(("entity -schedule -type feed -name "
						+ overlay.get("outputFeedName")).split("\\s")));

		Assert.assertEquals(0,
				new IvoryCLI().run(("entity -schedule -type process -name "
						+ overlay.get("processName")).split("\\s")));
		
	}
	
	@Test(enabled = enableTest)
	public void testGetVersion() throws Exception {
		Assert.assertEquals( 0,
				new IvoryCLI().run("admin -version".split("\\s")));
		
		Assert.assertEquals( 0,
				new IvoryCLI().run("admin -stack".split("\\s")));
	}
	
	@Test(enabled = enableTest)
	public void testInstanceGetLogs() throws Exception {
		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(0,
				executeWithURL("entity -schedule -type process -name "
						+ overlay.get("processName")));
		
		Assert.assertEquals(0,
				executeWithURL("instance -logs -type process -name "
						+ overlay.get("processName")
						+ " -start " + START_INSTANCE + " -end " + START_INSTANCE));
		
	}

	
	private int executeWithURL(String command) throws Exception {
		return new IvoryCLI()
				.run((command + " -url " + BASE_URL).split("\\s+"));
	}

	private String createTempJobPropertiesFile() throws IOException {
		File target = new File("webapp/target");
		if (!target.exists()) {
			target = new File("target");
		}
		File tmpFile = File.createTempFile("job", ".properties", target);
		OutputStream out = new FileOutputStream(tmpFile);
		out.write("oozie.wf.rerun.failnodes=true\n".getBytes());
		out.close();
		return tmpFile.getAbsolutePath();
	}

	public void submitTestFiles(Map<String, String> overlay) throws Exception {

		String filePath = overlayParametersOverTemplate(CLUSTER_FILE_TEMPLATE,
				overlay);
		Assert.assertEquals(
				0,
				executeWithURL("entity -submit -type cluster -file " + filePath));

		filePath = overlayParametersOverTemplate(FEED_TEMPLATE1, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));

		filePath = overlayParametersOverTemplate(FEED_TEMPLATE2, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));

		filePath = overlayParametersOverTemplate(PROCESS_TEMPLATE, overlay);
		Assert.assertEquals(
				0,
				executeWithURL("entity -submit -type process -file " + filePath));
	}

	private static class InMemoryWriter extends PrintStream {

		private StringBuffer buffer = new StringBuffer();

		public InMemoryWriter(OutputStream out) {
			super(out);
		}

		@Override
		public void println(String x) {
			clear();
			buffer.append(x);
			super.println(x);
		}

		public String getBuffer() {
			return buffer.toString();
		}

		public void clear() {
			buffer.delete(0, buffer.length());
		}
	}
}
