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

import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import jobmanager.database.MongoAccessor;
import jobmanager.messaging.handler.AbortJobHandler;
import jobmanager.messaging.handler.CreateJobHandler;
import jobmanager.messaging.handler.RepeatJobHandler;
import jobmanager.messaging.handler.RequestJobHandler;
import jobmanager.messaging.handler.UpdateStatusHandler;
import model.job.Job;
import model.job.JobProgress;
import model.job.result.type.TextResult;
import model.job.type.AbortJob;
import model.job.type.AccessJob;
import model.job.type.IngestJob;
import model.job.type.RepeatJob;
import model.request.PiazzaJobRequest;
import model.status.StatusUpdate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.test.util.ReflectionTestUtils;

import util.PiazzaLogger;
import util.UUIDFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tests the Job Handlers
 * 
 * @author Patrick.Doody
 *
 */
public class HandlerTests {
	@Mock
	private PiazzaLogger logger;
	@Mock
	private MongoAccessor accessor;
	@Mock
	private UUIDFactory uuidFactory;
	@Mock
	private Producer<String, String> producer;

	@InjectMocks
	private AbortJobHandler abortJobHandler;
	@InjectMocks
	private CreateJobHandler createJobHandler;
	@InjectMocks
	private RepeatJobHandler repeatJobHandler;
	@InjectMocks
	private RequestJobHandler requestJobHandler;
	@InjectMocks
	private UpdateStatusHandler updateJobHandler;

	private Job mockJob;
	private PiazzaJobRequest mockAbortRequest;
	private PiazzaJobRequest repeatJobRequest;

	/**
	 * Initialize Mock objects.
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		// Mock a Job
		mockJob = new Job();
		mockJob.jobId = UUID.randomUUID().toString();
		mockJob.status = StatusUpdate.STATUS_RUNNING;
		mockJob.progress = new JobProgress(75);
		mockJob.jobType = new IngestJob();

		// Mock Requests
		mockAbortRequest = new PiazzaJobRequest();
		mockAbortRequest.jobType = new AbortJob("123456");
		mockAbortRequest.userName = "A";

		repeatJobRequest = new PiazzaJobRequest();
		repeatJobRequest.userName = "A";
		repeatJobRequest.jobType = new RepeatJob("123456");

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
	 * Tests abort job handler with no Job found
	 */
	@Test(expected = Exception.class)
	public void testAbortJobEmpty() throws Exception {
		// Mock
		when(accessor.getJobById(eq("123456"))).thenReturn(null);

		// Test when no Job is found
		abortJobHandler.process(mockAbortRequest);
	}

	/**
	 * Tests when a user cannot abort someone elses Job
	 */
	@Test(expected = Exception.class)
	public void testAbortPermissionError() throws Exception {
		// Mock
		Job mockCancelJob = new Job();
		mockCancelJob.submitterUserName = "B";
		when(accessor.getJobById(eq("123456"))).thenReturn(mockCancelJob);

		// Test
		abortJobHandler.process(mockAbortRequest);
	}

	/**
	 * Tests the cancelling of an already aborted Job
	 */
	@Test(expected = Exception.class)
	public void testAbortCancelledJob() throws Exception {
		// Mock
		Job mockCancelJob = new Job();
		mockCancelJob.submitterUserName = "A";
		mockCancelJob.status = StatusUpdate.STATUS_CANCELLED;
		when(accessor.getJobById(eq("123456"))).thenReturn(mockCancelJob);

		// Test
		abortJobHandler.process(mockAbortRequest);
	}

	/**
	 * Tests aborting a Job, no errors.
	 */
	@Test
	public void testAbortJob() throws Exception {
		// Mock
		Job mockCancelJob = new Job();
		mockCancelJob.submitterUserName = "A";
		mockCancelJob.status = StatusUpdate.STATUS_RUNNING;
		when(accessor.getJobById(eq("123456"))).thenReturn(mockCancelJob);
		Mockito.doNothing().when(accessor).updateJobStatus(eq("123456"), eq(StatusUpdate.STATUS_CANCELLED));

		// Test
		abortJobHandler.process(mockAbortRequest);
	}

	/**
	 * Tests repeating a Job when no original Job is found
	 * 
	 * @throws Exception
	 */
	@Test(expected = Exception.class)
	public void testRepeatNoJobFound() throws Exception {
		// Mock
		when(accessor.getJobById(eq("123456"))).thenReturn(null);

		// Test
		repeatJobHandler.process(repeatJobRequest);
	}

	/**
	 * Test repeating a Job
	 * 
	 * @throws Exception
	 */
	@Test
	public void testRepeatJob() throws Exception {
		// Mock
		Job jobToRepeat = new Job();
		jobToRepeat.setJobId("123456");
		jobToRepeat.jobType = new AccessJob();
		when(accessor.getJobById(eq("123456"))).thenReturn(jobToRepeat);
		when(uuidFactory.getUUID()).thenReturn("654321");

		// Test
		String newJobId = repeatJobHandler.process(repeatJobRequest);
		assertTrue(newJobId.equals("654321"));
	}

	/**
	 * Tests the updating of a Status
	 */
	@Test
	public void testUpdateStatus() throws Exception {
		// Mock
		StatusUpdate mockStatus = new StatusUpdate(StatusUpdate.STATUS_RUNNING, new JobProgress(50));
		mockStatus.setResult(new TextResult("Done"));
		ConsumerRecord<String, String> mockRecord = new ConsumerRecord<String, String>("Request-Job", 0, 0, "123456",
				new ObjectMapper().writeValueAsString(mockStatus));
		Mockito.doNothing().when(accessor).updateJobStatus(eq("123456"), eq(StatusUpdate.STATUS_RUNNING));
		Mockito.doNothing().when(accessor).updateJobProgress(eq("123456"), any(JobProgress.class));
		when(accessor.getJobById(eq("123456"))).thenReturn(new Job());
		Mockito.doNothing().when(accessor).removeJob(eq("123456"));
		Mockito.doNothing().when(accessor).addJob(any(Job.class));

		// Test
		updateJobHandler.process(mockRecord);
	}
}