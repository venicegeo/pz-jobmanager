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

import jobmanager.database.MongoAccessor;
import messaging.job.JobMessageFactory;
import model.job.Job;
import model.job.result.type.ErrorResult;
import model.job.result.type.JobResult;
import model.job.type.RepeatJob;
import model.request.PiazzaJobRequest;
import model.status.StatusUpdate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import util.PiazzaLogger;
import util.UUIDFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Handles the request for Repeating a Job in the Job Table.
 * 
 * @author Patrick.Doody
 */
@Component
public class RepeatJobHandler {
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private MongoAccessor accessor;
	@Autowired
	private UUIDFactory uuidFactory;
	@Value("${SPACE}")
	private String SPACE;

	private Producer<String, String> producer;

	/**
	 * Sets the producer for this Handler. Uses injection from the Job Messager
	 * in order to be efficient in creating only one producer, as producers are
	 * thread-safe.
	 * 
	 * @param producer
	 *            The producer.
	 */
	public void setProducer(Producer<String, String> producer) {
		this.producer = producer;
	}

	@Deprecated
	public void process(ConsumerRecord<String, String> consumerRecord) throws Exception {
		try {
			// Flag the Status of this Job to indicate we are handling it.
			StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_RUNNING);
			ProducerRecord<String, String> updateJobMessage = JobMessageFactory.getUpdateStatusMessage(
					consumerRecord.key(), statusUpdate, SPACE);
			producer.send(updateJobMessage);

			// Deserialize the Job Request to the appropriate Models
			ObjectMapper mapper = new ObjectMapper();
			Job repeatJob = mapper.readValue(consumerRecord.value(), Job.class);
			RepeatJob repeatJobType = (RepeatJob) repeatJob.getJobType();
			String repeatJobId = repeatJobType.getJobId();

			// Lookup the Jobs table for the requested Job to Repeat. Ensure it
			// exists.
			Job job = accessor.getJobById(repeatJobId);
			if (job == null) {
				throw new Exception(String.format("Job %s could not be found.", repeatJobId));
			}

			// Create a new JobRequest object. The Submitter will be the user
			// who requested the Job to be repeated. The Job Type will be the
			// Type of the Job that is to be repeated.
			PiazzaJobRequest request = new PiazzaJobRequest();
			request.userName = repeatJob.submitterUserName;
			request.jobType = job.getJobType();

			// Dispatch the Message to Repeat the selected Job. Create an ID so
			// we can immediately attach a result to the RepeatJob request.
			String newRepeatJobId = uuidFactory.getUUID();
			ProducerRecord<String, String> repeatJobMessage = JobMessageFactory.getRequestJobMessage(request,
					newRepeatJobId, SPACE);
			producer.send(repeatJobMessage).get(); // Ensuring we get Exceptions
													// immediately

			// Update the Status of the RepeatJob request so that the result can
			// be linked to the newly created Job.
			statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);
			statusUpdate.setResult(new JobResult(newRepeatJobId));
			updateJobMessage = JobMessageFactory.getUpdateStatusMessage(repeatJob.getJobId(), statusUpdate, SPACE);
			producer.send(updateJobMessage);
		} catch (Exception exception) {
			// Log the error.
			String message = String.format("Could not Repeat Job %s due to reason: %s", consumerRecord.key(),
					exception.getMessage());
			logger.log(message, PiazzaLogger.ERROR);
			exception.printStackTrace();

			// Send the Status that this RepeatJob Request has failed.
			StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_ERROR);
			statusUpdate.setResult(new ErrorResult("Error Repeating Job.", message));
			ProducerRecord<String, String> updateJobMessage = JobMessageFactory.getUpdateStatusMessage(
					consumerRecord.key(), statusUpdate, SPACE);
			producer.send(updateJobMessage);
		}
	}

	/**
	 * Processes a job request to repeat a Job within Piazza.
	 * 
	 * @param request
	 *            The request, detailing the user and the job to be repeated.
	 * @return The ID of the newly created Job.
	 */
	public String process(PiazzaJobRequest request) throws Exception {
		RepeatJob repeatJob = (RepeatJob) request.jobType;
		String repeatJobId = repeatJob.getJobId();

		// Lookup the Jobs table for the requested Job to Repeat. Ensure it
		// exists.
		Job job = accessor.getJobById(repeatJobId);
		if (job == null) {
			throw new Exception(String.format("Job %s could not be found.", repeatJobId));
		}

		// Create a new JobRequest object. The Submitter will be the user
		// who requested the Job to be repeated. The Job Type will be the
		// Type of the Job that is to be repeated.
		PiazzaJobRequest newJobRequest = new PiazzaJobRequest();
		newJobRequest.userName = job.submitterUserName;
		newJobRequest.jobType = job.getJobType();

		// Dispatch the Message to Repeat the selected Job. Create an ID so
		// we can immediately attach a result to the RepeatJob request.
		String newRepeatJobId = uuidFactory.getUUID();
		ProducerRecord<String, String> repeatJobMessage = JobMessageFactory.getRequestJobMessage(newJobRequest,
				newRepeatJobId, SPACE);
		producer.send(repeatJobMessage).get(); // Ensuring we get Exceptions
												// immediately

		return newRepeatJobId;
	}
}
