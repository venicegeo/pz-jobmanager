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
import model.job.Job;
import model.job.type.AbortJob;
import model.request.PiazzaJobRequest;
import model.status.StatusUpdate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mongojack.DBQuery;
import org.mongojack.DBUpdate;

import util.PiazzaLogger;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Handles the request for Aborting a Job by updating the Job table with the new
 * Status.
 * 
 * @author Patrick.Doody
 * 
 */
public class AbortJobHandler {
	private PiazzaLogger logger;
	private MongoAccessor accessor;

	public AbortJobHandler(MongoAccessor accessor, PiazzaLogger logger) {
		this.accessor = accessor;
		this.logger = logger;
	}

	public void process(ConsumerRecord<String, String> consumerRecord) {
		// Changing the Status in the Job Table to Aborted
		try {
			ObjectMapper mapper = new ObjectMapper();
			PiazzaJobRequest job = mapper.readValue(consumerRecord.value(), PiazzaJobRequest.class);
			// Update the status of the Job. Only update if the Job is currently
			// pending or running. Otherwise, we cannot cancel a completed Job.
			String jobId = ((AbortJob)job.jobType).jobId;
			Job jobToCancel = accessor.getJobById(jobId);

			if( jobToCancel.submitterUserName == null ) {
				logger.log(String.format("Could not Abort Job %s because it does not have a user associated with it", jobId),
						PiazzaLogger.WARNING);
				return;
				
			} else if ( !jobToCancel.submitterUserName.equals(job.userName) ){
				logger.log(String.format("Could not Abort Job %s because it was not created by user requesting abort.", jobId),
						PiazzaLogger.WARNING);
				return;
			}
			
			String currentStatus = jobToCancel.status;
			if ((currentStatus.equals(StatusUpdate.STATUS_RUNNING))
					|| (currentStatus.equals(StatusUpdate.STATUS_PENDING))
					|| (currentStatus.equals(StatusUpdate.STATUS_SUBMITTED))) {
				accessor.getJobCollection().update(DBQuery.is("jobId", jobId),
						DBUpdate.set("status", StatusUpdate.STATUS_CANCELLED));
				logger.log(
						String.format("Aborted the Job %s of Abort Job ID %s", jobId, consumerRecord.key()),
						PiazzaLogger.INFO);
			} else {
				logger.log(String.format("Could not Abort Job %s because it is no longer running.", jobId),
						PiazzaLogger.WARNING);
			}

		} catch (Exception exception) {
			logger.log(
					String.format("Error setting Aborted status for Job %s: %s", consumerRecord.key(),
							exception.getMessage()), PiazzaLogger.ERROR);
			exception.printStackTrace();
		}
	}
}
