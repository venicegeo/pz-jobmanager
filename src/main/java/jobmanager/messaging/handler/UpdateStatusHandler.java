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
import model.status.StatusUpdate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mongojack.DBQuery;
import org.mongojack.DBUpdate;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Handles the request for Updating the Status of a Job in the Job Table
 * 
 * @author Patrick.Doody
 * 
 */
public class UpdateStatusHandler {
	private MongoAccessor accessor;

	public UpdateStatusHandler(MongoAccessor accessor) {
		this.accessor = accessor;
	}

	public void process(ConsumerRecord<String, String> consumerRecord) {
		// Changing the Status in the Job Table
		try {
			ObjectMapper mapper = new ObjectMapper();
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
				accessor.getJobCollection().update(DBQuery.is("jobId", consumerRecord.key()),
						DBUpdate.set("result", statusUpdate.getResult()));
			}
		} catch (Exception exception) {
			System.out.println("Error updating Job status in Collection: " + exception.getMessage());
			exception.printStackTrace();
		}
	}
}
