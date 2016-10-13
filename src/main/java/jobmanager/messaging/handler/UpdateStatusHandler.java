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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import jobmanager.database.MongoAccessor;
import model.status.StatusUpdate;
import util.PiazzaLogger;

/**
 * Handles the request for Updating the Status of a Job in the Job Table
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class UpdateStatusHandler {
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private MongoAccessor accessor;

	private final static Logger LOGGER = LoggerFactory.getLogger(UpdateStatusHandler.class);
	ObjectMapper mapper = new ObjectMapper();

	@Async
	public void process(ConsumerRecord<String, String> consumerRecord) {
		// Changing the Status in the Job Table
		try {
			// Get the Status wrapper that contains the updates to be applied
			StatusUpdate statusUpdate = mapper.readValue(consumerRecord.value(), StatusUpdate.class);

			// Update
			accessor.updateJobStatus(consumerRecord.key(), statusUpdate);

			// Log success
			logger.log(String.format("Processed Update Status for Job %s with Status %s.", consumerRecord.key(), statusUpdate.getStatus()),
					PiazzaLogger.INFO);
		} catch (Exception exception) {
			String error = String.format("Error Updating Status for Job %s with error %s", consumerRecord.key(), exception.getMessage());
			LOGGER.error(error, exception);
			logger.log(error, PiazzaLogger.ERROR);
		}
	}
}
