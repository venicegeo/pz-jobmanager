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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import jobmanager.database.MongoAccessor;
import jobmanager.messaging.JobMessager;
import jobmanager.messaging.handler.AbortJobHandler;
import jobmanager.messaging.handler.RepeatJobHandler;
import jobmanager.messaging.handler.RequestJobHandler;
import jobmanager.messaging.handler.UpdateStatusHandler;
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
	private UpdateStatusHandler updateStatusHandler;
	@Mock
	private RepeatJobHandler repeatJobHandler;
	@Mock
	private RequestJobHandler requestJobHandler;
	@Mock
	private Consumer<String, String> consumer;

	@InjectMocks
	private JobMessager jobMessager;

	/**
	 * Setup tests
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		// Mock the values for the Topic names
		ReflectionTestUtils.setField(jobMessager, "UPDATE_JOB_TOPIC_NAME", "Update-Job");
		ReflectionTestUtils.setField(jobMessager, "REQUEST_JOB_TOPIC_NAME", "Request-Job");
		ReflectionTestUtils.setField(jobMessager, "KAFKA_ADDRESS", "localhost:9092");
		ReflectionTestUtils.setField(jobMessager, "SPACE", "unit-test");
		ReflectionTestUtils.setField(jobMessager, "KAFKA_GROUP", "job-unit-test");
	}

	/**
	 * Testing that Consumer Records are appropriately handled
	 */
	@Test
	public void testProcessing() throws Exception {
		// Create Kafka messages to pass to the Messager
		ConsumerRecord<String, String> update = new ConsumerRecord<String, String>("Update-Job", 0, 0, null, null);
		ConsumerRecord<String, String> request = new ConsumerRecord<String, String>("Request-Job", 0, 0, null, null);

		// Verify the Messages are appropriately handled
		Mockito.doNothing().when(updateStatusHandler).process(any(ConsumerRecord.class));
		jobMessager.processMessage(update);

		Mockito.doNothing().when(requestJobHandler).process(any(ConsumerRecord.class));
		jobMessager.processMessage(request);
	}

	/**
	 * Test initialization and listening loop
	 */
	@Test
	public void testInitListening() throws Exception {
		// Mock
		Mockito.doNothing().when(consumer).subscribe(anyList());
		ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<String, String>(null);
		Mockito.when(consumer.poll(anyLong())).thenReturn(consumerRecords);

		// Test
		jobMessager.initialize();

		// Stop the listening Thread
		jobMessager.stopPolling();
	}
}