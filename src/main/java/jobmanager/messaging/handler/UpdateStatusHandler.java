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
import model.status.StatusUpdate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mongojack.DBQuery;
import org.mongojack.DBUpdate;

import util.PiazzaLogger;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Handles the request for Updating the Status of a Job in the Job Table
 * 
 * @author Patrick.Doody
 * 
 */
public class UpdateStatusHandler {
	private PiazzaLogger logger;
	private MongoAccessor accessor;

	public UpdateStatusHandler(MongoAccessor accessor, PiazzaLogger logger) {
		this.accessor = accessor;
		this.logger = logger;
	}

	public synchronized void process(ConsumerRecord<String, String> consumerRecord) {
		// Changing the Status in the Job Table
		try {
			ObjectMapper mapper = new ObjectMapper();
			// Get the Status wrapper that contains the updates to be applied
			StatusUpdate statusUpdate = mapper.readValue(consumerRecord.value(), StatusUpdate.class);

			// Update the Job Status, if specified
			if (!statusUpdate.getStatus().isEmpty()) {
				accessor.getJobCollection().update(DBQuery.is("jobId", consumerRecord.key()),
						DBUpdate.set("status", statusUpdate.getStatus()));
			}

			// Update the Job progress, if specified
			if (statusUpdate.getProgress() != null) {
				accessor.getJobCollection().update(DBQuery.is("jobId", consumerRecord.key()),
						DBUpdate.set("progress", statusUpdate.getProgress()));
			}

			// Update the Result, if specified
			if (statusUpdate.getResult() != null) {
				// Get the Job to apply the Result to
				Job job = accessor.getJobById(consumerRecord.key());
				// Update the Job Result
				job.result = statusUpdate.getResult();
				/**
				 * It is important to note that we are not doing an update of
				 * the Mongo Resource here, as one would expect. This is due to
				 * a bug in MongoJack, documented here:
				 * https://github.com/mongojack/mongojack/issues/101; that
				 * explains how updating of MongoJack collections with
				 * polymorphic objects currently only serializes the fields
				 * found in the parent class or interface, and all child fields
				 * are ignored.
				 * 
				 * This is important for us because the Results of a Job are
				 * polymorphic (specifically, the ResultType interface) and thus
				 * are not getting properly serialized as a result of this bug.
				 * This bug exists in all versions of MongoJack and is still
				 * OPEN in GitHub issues.
				 * 
				 * Due to this issue, we are tagging this method as
				 * synchronized, and updating the Job properties in a Job
				 * object, and then deleting that object from the database and
				 * immediately committing the new Job with the updates. The
				 * above-mentioned bug only affects updates, so the work-around
				 * here is avoiding updates by creating a new object in the
				 * database. This is functionally acceptable because we make no
				 * use of MongoDB's primary key - our key is based on the JobID
				 * property, which is maintained throughout the transaction.
				 */
				// Delete the existing entry for the Job
				accessor.getJobCollection().remove(DBQuery.is("jobId", consumerRecord.key()));
				// Re-add the Job Entry
				accessor.getJobCollection().insert(job);
			}
			logger.log(String.format("Processed Update Status for Job %s with Status %s", consumerRecord.key(),
					statusUpdate.getStatus()), PiazzaLogger.INFO);
		} catch (Exception exception) {
			logger.log(String.format("Error Updating Status for Job %s", consumerRecord.key()), PiazzaLogger.ERROR);
			exception.printStackTrace();
		}
	}
}
