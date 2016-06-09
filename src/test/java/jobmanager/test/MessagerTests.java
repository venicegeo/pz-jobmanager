/**
 * Copyright 2016, RadiantBlue Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package jobmanager.test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jobmanager.database.MongoAccessor;
import jobmanager.messaging.JobMessager;
import jobmanager.messaging.handler.AbortJobHandler;
import jobmanager.messaging.handler.CreateJobHandler;
import jobmanager.messaging.handler.RepeatJobHandler;
import jobmanager.messaging.handler.RequestJobHandler;
import jobmanager.messaging.handler.UpdateStatusHandler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import util.PiazzaLogger;
import util.UUIDFactory;

/**
 * Tests the Job Messager
 * 
 * @author Patrick.Doody
 *
 */
public class MessagerTests {
	@Mock
	private PiazzaLogger logger;
	@Mock
	private UUIDFactory uuidFactory;
	@Mock
	private MongoAccessor accessor;
	@Mock
	private AbortJobHandler abortJobHandler;
	@Mock
	private CreateJobHandler createJobHandler;
	@Mock
	private UpdateStatusHandler updateStatusHandler;
	@Mock
	private RepeatJobHandler repeatJobHandler;
	@Mock
	private RequestJobHandler requestJobHandler;

	@InjectMocks
	private JobMessager jobMessager;

	/**
	 * Setup tests
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		// Mock the values for the Topic names
		ReflectionTestUtils.setField(jobMessager, "CREATE_JOB_TOPIC_NAME", "Create-Job");
		ReflectionTestUtils.setField(jobMessager, "ABORT_JOB_TOPIC_NAME", "Abort-Job");
		ReflectionTestUtils.setField(jobMessager, "UPDATE_JOB_TOPIC_NAME", "Update-Job");
		ReflectionTestUtils.setField(jobMessager, "REQUEST_JOB_TOPIC_NAME", "Request-Job");
		ReflectionTestUtils.setField(jobMessager, "REPEAT_JOB_TOPIC_NAME", "repeat");
	}

	/**
	 * Testing that Consumer Records are appropriately handled
	 */
	@Test
	public void testProcessing() throws Exception {
		// Create Kafka messages to pass to the Messager
		ConsumerRecord<String, String> create = new ConsumerRecord<String, String>("Create-Job", 0, 0, null, null);
		ConsumerRecord<String, String> abort = new ConsumerRecord<String, String>("Abort-Job", 0, 0, null, null);
		ConsumerRecord<String, String> update = new ConsumerRecord<String, String>("Update-Job", 0, 0, null, null);
		ConsumerRecord<String, String> request = new ConsumerRecord<String, String>("Request-Job", 0, 0, null, null);
		ConsumerRecord<String, String> repeat = new ConsumerRecord<String, String>("repeat", 0, 0, null, null);

		// Verify the Messages are appropriately handled
		Mockito.doNothing().when(createJobHandler).process(any(ConsumerRecord.class));
		jobMessager.processMessage(create);

		Mockito.doNothing().when(abortJobHandler).process(any(ConsumerRecord.class));
		jobMessager.processMessage(abort);

		Mockito.doNothing().when(updateStatusHandler).process(any(ConsumerRecord.class));
		jobMessager.processMessage(update);

		Mockito.doNothing().when(requestJobHandler).process(any(ConsumerRecord.class));
		jobMessager.processMessage(request);

		Mockito.doNothing().when(repeatJobHandler).process(any(ConsumerRecord.class));
		jobMessager.processMessage(repeat);

	}
}