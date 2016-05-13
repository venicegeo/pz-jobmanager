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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import util.PiazzaLogger;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Creates corresponding entries in the Jobs table for the Kafka Message
 * 
 * When the old API has been completely removed, this handler will be deprecated
 * completely by the Request Job handler. All logic will happen in that handler;
 * and this one can be removed.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class CreateJobHandler {
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private MongoAccessor accessor;

	@Deprecated
	public void process(ConsumerRecord<String, String> consumerRecord) {
		// Inserting Job Information into the Job Table
		try {
			ObjectMapper mapper = new ObjectMapper();
			Job job = mapper.readValue(consumerRecord.value(), Job.class);
			process(job);
		} catch (Exception exception) {
			logger.log(
					String.format("Error Adding Job %s to Job Table: %s", consumerRecord.key(), exception.getMessage()),
					PiazzaLogger.ERROR);
			exception.printStackTrace();
		}
	}

	/**
	 * Adds Job information to the Job table.
	 * 
	 * <p>
	 * When the old API has been completely removed, this handler will be
	 * deprecated completely by the Request Job handler. All logic will happen
	 * in that handler; and this one can be removed.
	 * </p>
	 * 
	 * @param job
	 *            The job to add.
	 */
	@Deprecated
	public void process(Job job) {
		accessor.getJobCollection().insert(job);
		logger.log(
				String.format("Indexed Job %s with Type %s to Job Table.", job.getJobId(), job.getJobType().getType()),
				PiazzaLogger.INFO);
	}
}