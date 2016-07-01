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
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import jobmanager.controller.JobController;
import jobmanager.database.MongoAccessor;
import jobmanager.messaging.handler.AbortJobHandler;
import jobmanager.messaging.handler.CreateJobHandler;
import jobmanager.messaging.handler.RepeatJobHandler;
import model.job.Job;
import model.job.JobProgress;
import model.job.type.AbortJob;
import model.job.type.IngestJob;
import model.job.type.RepeatJob;
import model.request.PiazzaJobRequest;
import model.response.ErrorResponse;
import model.response.JobErrorResponse;
import model.response.JobResponse;
import model.response.JobStatusResponse;
import model.response.PiazzaResponse;
import model.status.StatusUpdate;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import util.PiazzaLogger;

/**
 * Tests the Job Controller REST Endpoint, which returns Job Status and other
 * Job Table information.
 * 
 * @author Patrick.Doody
 * 
 */
public class JobControllerTests {
	@Mock
	private PiazzaLogger logger;
	@Mock
	private MongoAccessor accessor;
	@Mock
	private CreateJobHandler createJobHandler;
	@Mock
	private AbortJobHandler abortJobHandler;
	@Mock
	private RepeatJobHandler repeatJobHandler;
	@InjectMocks
	private JobController jobController;

	private Job mockJob;
	private List<Job> mockJobs;

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

		// Mock a list of Jobs
		mockJobs = new ArrayList<Job>();
		mockJobs.add(mockJob);
	}

	/**
	 * Test / root endpoint
	 */
	@Test
	public void testHealthCheck() {
		assertTrue(jobController.getHealthCheck().contains("Health Check"));
	}

	/**
	 * Test /job/{jobId}
	 */
	@Test
	public void testStatus() {
		// Test error handling on Null Job ID
		PiazzaResponse response = jobController.getJobStatus(null);
		assertTrue(response instanceof JobErrorResponse);

		// When we query the Status of the Mock Job's ID, return the Mock Job
		when(accessor.getJobById(mockJob.jobId)).thenReturn(mockJob);

		// Query the Job
		response = jobController.getJobStatus(mockJob.jobId);
		assertTrue(response instanceof JobStatusResponse);
		JobStatusResponse jobStatus = (JobStatusResponse) response;
		assertTrue(jobStatus.jobId.equals(mockJob.getJobId()));
		assertTrue(jobStatus.progress.equals(mockJob.progress));
		assertTrue(jobStatus.status.equals(StatusUpdate.STATUS_RUNNING));
		assertTrue(jobStatus.jobType.equals(mockJob.getJobType().getClass().getSimpleName()));

		// Test Job Not Exists
		when(accessor.getJobById(mockJob.jobId)).thenReturn(null);
		response = jobController.getJobStatus(mockJob.jobId);
		assertTrue(response instanceof JobErrorResponse);
	}

	/**
	 * Test /createJob
	 */
	@Test
	public void testCreateJob() throws Exception {
		// Mock
		Mockito.doNothing().when(createJobHandler).process(any(Job.class));

		// Test
		PiazzaResponse response = jobController.createJob(mockJob);

		// Verify
		assertTrue(response == null);

		// Test Exception
		Mockito.doThrow(new Exception("Couldn't Create")).when(createJobHandler).process(any(Job.class));
		response = jobController.createJob(mockJob);
		assertTrue(response instanceof JobErrorResponse);
		assertTrue(((JobErrorResponse) response).message.contains("Couldn't Create"));
	}

	/**
	 * Test /abort
	 */
	@Test
	public void testAbortJob() throws Exception {
		// Mock
		Mockito.doNothing().when(abortJobHandler).process(any(PiazzaJobRequest.class));
		PiazzaJobRequest mockRequest = new PiazzaJobRequest();
		mockRequest.jobType = new AbortJob("123456");

		// Test
		PiazzaResponse response = jobController.abortJob(mockRequest);

		// Verify
		assertTrue(response == null);

		// Test Exception
		Mockito.doThrow(new Exception("Couldn't Abort")).when(abortJobHandler).process(any(PiazzaJobRequest.class));
		response = jobController.abortJob(new PiazzaJobRequest());
		assertTrue(response instanceof ErrorResponse);
		assertTrue(((ErrorResponse) response).message.contains("Couldn't Abort"));
	}

	/**
	 * Test /repeat
	 */
	@Test
	public void testRepeat() throws Exception {
		// Mock
		when(repeatJobHandler.process(any(PiazzaJobRequest.class))).thenReturn("123456");
		PiazzaJobRequest mockRequest = new PiazzaJobRequest();
		mockRequest.jobType = new RepeatJob("123456");

		// Test
		PiazzaResponse response = jobController.repeatJob(mockRequest);

		// Verify
		assertTrue(response instanceof JobResponse);
		assertTrue(((JobResponse)response).jobId.equals("123456"));

		// Test Exception
		Mockito.doThrow(new Exception("Can't Repeat")).when(repeatJobHandler).process(any(PiazzaJobRequest.class));
		response = jobController.repeatJob(new PiazzaJobRequest());
		assertTrue(response instanceof ErrorResponse);
		assertTrue(((ErrorResponse) response).message.contains("Can't Repeat"));
	}

	/**
	 * Test /job
	 */
	@Test
	public void testGetJobs() {
		// Mock
		when(accessor.getJobs(anyInt(), anyInt(), anyString())).thenReturn(mockJobs);

		// Test
		List<Job> jobs = jobController.getJobs("0", "10", "ascending");
		// Verify
		assertTrue(jobs != null);
		assertTrue(jobs.size() == 1);
		assertTrue(jobs.get(0).getJobId().equals(mockJob.getJobId()));
	}

	/**
	 * Tests /job/status
	 */
	@Test
	public void testStatuses() {
		// Test
		List<String> statuses = jobController.getStatuses();

		// Verify
		assertTrue(statuses.size() == 7);
		assertTrue(statuses.contains(StatusUpdate.STATUS_CANCELLED));
		assertTrue(statuses.contains(StatusUpdate.STATUS_ERROR));
		assertTrue(statuses.contains(StatusUpdate.STATUS_FAIL));
		assertTrue(statuses.contains(StatusUpdate.STATUS_PENDING));
		assertTrue(statuses.contains(StatusUpdate.STATUS_RUNNING));
		assertTrue(statuses.contains(StatusUpdate.STATUS_SUBMITTED));
		assertTrue(statuses.contains(StatusUpdate.STATUS_SUCCESS));
	}

	/**
	 * Test /admin/stats
	 */
	@Test
	public void testAdminStats() {
		// Mock
		when(accessor.getJobsCount()).thenReturn(new Long(10));
		when(accessor.getJobStatusCount(anyString())).thenReturn(10);

		// Test
		ResponseEntity<Map<String, Object>> entity = jobController.getAdminStats();

		// Verify
		assertTrue(entity.getStatusCode().equals(HttpStatus.OK));
		Map<String, Object> stats = entity.getBody();
		assertTrue(stats.keySet().size() >= 8);
	}

	/**
	 * Tests /job/status/{status}
	 */
	@Test
	public void testJobStatus() {
		// Mock0
		when(accessor.getJobsForStatus(anyInt(), anyInt(), eq("ascending"), eq(StatusUpdate.STATUS_RUNNING)))
				.thenReturn(mockJobs);

		// Test
		List<Job> jobs = jobController.getJobsByStatus("0", "10", StatusUpdate.STATUS_RUNNING, "ascending");
		// Verify
		assertTrue(jobs != null);
		assertTrue(jobs.size() == 1);
		assertTrue(jobs.get(0).getJobId().equals(mockJob.getJobId()));
	}

	/**
	 * Tests /job/userName/{userName}
	 */
	@Test
	public void testJobUser() {
		// Mock
		when(accessor.getJobsForUser(anyInt(), anyInt(), eq("ascending"), eq("UNAUTHENTICATED"))).thenReturn(mockJobs);

		// Test
		List<Job> jobs = jobController.getJobsByUserName("0", "10", "UNAUTHENTICATED", "ascending");
		// Verify
		assertTrue(jobs != null);
		assertTrue(jobs.size() == 1);
		assertTrue(jobs.get(0).getJobId().equals(mockJob.getJobId()));
	}
}
