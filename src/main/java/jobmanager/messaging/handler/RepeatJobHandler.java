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
package jobmanager.messaging.handler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;

import exception.PiazzaJobException;
import model.job.Job;
import model.request.PiazzaJobRequest;

/**
 * Handles the request for Repeating a Job in the Job Table.
 * 
 * @author Patrick.Doody
 */
@Component
public class RepeatJobHandler {
	@Autowired
	private RequestJobHandler requestJobHandler;

	@Value("${SPACE}")
	private String SPACE;

	/**
	 * Processes a job request to repeat a Job within Piazza.
	 * 
	 * @param request
	 *            The request, detailing the user and the job to be repeated.
	 */
	@Async
	public void process(Job job, String newRepeatJobId) {
		// Create a new JobRequest object. The Submitter will be the user
		// who requested the Job to be repeated. The Job Type will be the
		// Type of the Job that is to be repeated.
		PiazzaJobRequest newJobRequest = new PiazzaJobRequest();
		newJobRequest.createdBy = job.getCreatedBy();
		newJobRequest.jobType = job.getJobType();
		newJobRequest.jobId = newRepeatJobId;

		// Process the Job Request
		requestJobHandler.process(newJobRequest, newRepeatJobId);
	}
}