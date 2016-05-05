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

import util.PiazzaLogger;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Creates corresponding entries in the Jobs table for the Kafka Message
 * 
 * @author Patrick.Doody
 * 
 */
public class CreateJobHandler {
	private PiazzaLogger logger;
	private MongoAccessor accessor;

	public CreateJobHandler(MongoAccessor accessor, PiazzaLogger logger) {
		this.accessor = accessor;
		this.logger = logger;
	}

	public void process(ConsumerRecord<String, String> consumerRecord) {
		// Inserting Job Information into the Job Table
		try {
logger.log(String.format("**Unmarshalling create job message"), PiazzaLogger.INFO);
			ObjectMapper mapper = new ObjectMapper();
			Job job = mapper.readValue(consumerRecord.value(), Job.class);
logger.log(String.format("**Unmarshalling complete"), PiazzaLogger.INFO);

logger.log(String.format("**Persisting job metadata into mongo"), PiazzaLogger.INFO);
			accessor.getJobCollection().insert(job);
logger.log(String.format("**Persisting job metadata into mongo complete"), PiazzaLogger.INFO);
			logger.log(String.format("Indexed Job %s with Type %s to Job Table.", job.getJobId(), job.getJobType()
					.getType()), PiazzaLogger.INFO);
		} catch (Exception exception) {
			logger.log(
					String.format("Error Adding Job %s to Job Table: %s", consumerRecord.key(), exception.getMessage()),
					PiazzaLogger.ERROR);
			exception.printStackTrace();
		}
	}
}