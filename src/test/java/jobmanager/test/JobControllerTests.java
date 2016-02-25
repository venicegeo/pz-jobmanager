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
import static org.mockito.Mockito.when;

import java.util.UUID;

import jobmanager.controller.JobController;
import jobmanager.database.MongoAccessor;
import model.job.Job;
import model.job.JobProgress;
import model.response.ErrorResponse;
import model.response.JobStatusResponse;
import model.response.PiazzaResponse;
import model.status.StatusUpdate;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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
	@InjectMocks
	private JobController jobController;

	/**
	 * Initialize Mock objects.
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
	}

	/**
	 * Tests the fetching of a Job Status.
	 */
	@Test
	public void getStatusTest() {
		// Test error handling on Null Job ID
		PiazzaResponse response = jobController.getJobStatus(null);
		assertTrue(response instanceof ErrorResponse);

		// Mock a Job to get the Status for.
		Job mockJob = new Job();
		mockJob.jobId = UUID.randomUUID().toString();
		mockJob.status = StatusUpdate.STATUS_RUNNING;
		mockJob.progress = new JobProgress(75);

		// When we query the Status of the Mock Job's ID, return the Mock Job
		when(accessor.getJobById(mockJob.jobId)).thenReturn(mockJob);

		// Query the Job
		response = jobController.getJobStatus(mockJob.jobId);
		assertTrue(response instanceof JobStatusResponse);
		JobStatusResponse jobStatus = (JobStatusResponse) response;
		assertTrue(jobStatus.jobId.equals(mockJob.getJobId()));
		assertTrue(jobStatus.progress.equals(mockJob.progress));
		assertTrue(jobStatus.status.equals(StatusUpdate.STATUS_RUNNING));
	}
}
