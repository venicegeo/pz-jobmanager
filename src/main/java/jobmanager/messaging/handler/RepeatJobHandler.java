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

import messaging.job.JobMessageFactory;
import model.job.Job;
import model.request.PiazzaJobRequest;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Handles the request for Repeating a Job in the Job Table.
 * 
 * @author Patrick.Doody
 */
@Component
public class RepeatJobHandler {
	@Value("${SPACE}")
	private String SPACE;

	private Producer<String, String> producer;

	/**
	 * Sets the producer for this Handler. Uses injection from the Job Messager in order to be efficient in creating
	 * only one producer, as producers are thread-safe.
	 * 
	 * @param producer
	 *            The producer.
	 */
	public void setProducer(Producer<String, String> producer) {
		this.producer = producer;
	}

	/**
	 * Processes a job request to repeat a Job within Piazza.
	 * 
	 * @param request
	 *            The request, detailing the user and the job to be repeated.
	 */
	@Async
	public void process(Job job, String newRepeatJobId) throws Exception {
		// Create a new JobRequest object. The Submitter will be the user
		// who requested the Job to be repeated. The Job Type will be the
		// Type of the Job that is to be repeated.
		PiazzaJobRequest newJobRequest = new PiazzaJobRequest();
		newJobRequest.createdBy = job.createdBy;
		newJobRequest.jobType = job.getJobType();

		// Dispatch the Message to Repeat the selected Job. Create an Id so
		// we can immediately attach a result to the RepeatJob request.
		ProducerRecord<String, String> repeatJobMessage = JobMessageFactory.getRequestJobMessage(newJobRequest, newRepeatJobId, SPACE);
		producer.send(repeatJobMessage);
	}
}