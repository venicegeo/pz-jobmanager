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

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.fasterxml.jackson.databind.ObjectMapper;

import jobmanager.database.MongoAccessor;
import jobmanager.messaging.handler.RequestJobHandler;
import model.job.type.RepeatJob;
import model.request.PiazzaJobRequest;
import util.PiazzaLogger;
import util.UUIDFactory;

/**
 * Tests the Handler for requesting Jobs
 * 
 * @author Patrick.Doody
 *
 */
public class RequestHandlerTests {
	@Mock
	private PiazzaLogger logger;
	@Mock
	private MongoAccessor accessor;
	@Mock
	private UUIDFactory uuidFactory;
	@Mock
	private Producer<String, String> producer;

	@InjectMocks
	private RequestJobHandler requestJobHandler;

	/**
	 * Initialization
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		// Mock the Kafka response that Producers will send. This will always
		// return a Future that completes immediately and simply returns true.
		when(producer.send(isA(ProducerRecord.class))).thenAnswer(new Answer<Future<Boolean>>() {
			@Override
			public Future<Boolean> answer(InvocationOnMock invocation) throws Throwable {
				Future<Boolean> future = mock(FutureTask.class);
				when(future.isDone()).thenReturn(true);
				when(future.get()).thenReturn(true);
				return future;
			}
		});
	}

	/**
	 * Test requesting a Job
	 */
	@Test
	public void testRequestJob() throws Exception {
		// Mock
		PiazzaJobRequest mockRequest = new PiazzaJobRequest();
		mockRequest.jobType = new RepeatJob("123456");

		when(uuidFactory.getUUID()).thenReturn("654321");
		ConsumerRecord<String, String> record = new ConsumerRecord<String, String>("Request-Job", 0, 0, new String(),
				new ObjectMapper().writeValueAsString(mockRequest));
		requestJobHandler.setProducer(producer);

		// Test
		requestJobHandler.process(record);
	}
}
