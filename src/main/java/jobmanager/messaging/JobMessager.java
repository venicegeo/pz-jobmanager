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
package jobmanager.messaging;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import jobmanager.database.MongoAccessor;
import jobmanager.messaging.handler.AbortJobHandler;
import jobmanager.messaging.handler.CreateJobHandler;
import jobmanager.messaging.handler.RepeatJobHandler;
import jobmanager.messaging.handler.UpdateStatusHandler;
import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import util.PiazzaLogger;
import util.UUIDFactory;

/**
 * Interacts with the Jobs Collection in the Mongo database based on Kafka
 * messages received.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class JobMessager {
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private UUIDFactory uuidFactory;
	@Autowired
	private MongoAccessor accessor;
	@Value("${vcap.services.pz-kafka.credentials.host:kafka.dev:9092}")
	private String KAFKA_ADDRESS;
	private String KAFKA_GROUP;
	private final String REPEAT_JOB_TYPE = "repeat";
	private Producer<String, String> producer;
	private Consumer<String, String> consumer;
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private AbortJobHandler abortJobHandler;
	private CreateJobHandler createJobHandler;
	private UpdateStatusHandler updateStatusHandler;
	private RepeatJobHandler repeatJobHandler;

	/**
	 * Expected for Component instantiation
	 */
	public JobMessager() {
	}

	@PostConstruct
	public void initialize() {
		String KAFKA_HOST = KAFKA_ADDRESS.split(":")[0];
		String KAFKA_PORT = KAFKA_ADDRESS.split(":")[1];
		// Initialize the Consumer and Producer
		consumer = KafkaClientFactory.getConsumer(KAFKA_HOST, KAFKA_PORT, KAFKA_GROUP);
		producer = KafkaClientFactory.getProducer(KAFKA_HOST, KAFKA_PORT);
		// Initialize Handlers
		abortJobHandler = new AbortJobHandler(accessor, logger);
		createJobHandler = new CreateJobHandler(accessor, logger);
		updateStatusHandler = new UpdateStatusHandler(accessor, logger);
		repeatJobHandler = new RepeatJobHandler(accessor, producer, logger, uuidFactory);
		// Immediately Poll on a new thread
		Thread pollThread = new Thread() {
			public void run() {
				poll();
			}
		};
		pollThread.start();
	}

	/**
	 * Begins polling Kafka messages for consumption.
	 */
	public void poll() {
		try {
			// Subscribe to all Topics of concern
			consumer.subscribe(Arrays.asList(JobMessageFactory.CREATE_JOB_TOPIC_NAME,
					JobMessageFactory.UPDATE_JOB_TOPIC_NAME, JobMessageFactory.ABORT_JOB_TOPIC_NAME, REPEAT_JOB_TYPE));
			// Continuously poll for these topics
			while (!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
				// Handle new Messages on this topic.
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					try {
						processMessage(consumerRecord);
					} catch (Exception exception) {
						exception.printStackTrace();
						logger.log(String.format("Error processing Job with Key %s under Topic %s",
								consumerRecord.key(), consumerRecord.topic()), PiazzaLogger.ERROR);
					}
				}
			}
		} catch (WakeupException exception) {
			logger.log(String.format("Job Listener Thread forcefully shut: %s", exception.getMessage()),
					PiazzaLogger.FATAL);
			// Ignore exception if closing
			if (!closed.get()) {
				throw exception;
			}
		} finally {
			consumer.close();
		}
	}

	/**
	 * Processes an incoming Kafka Message. This will pass it off to the
	 * appropriate handler that will make the necessary changes to the Jobs
	 * Table.
	 * 
	 * @param consumerRecord
	 *            The message to process.
	 */
	public void processMessage(ConsumerRecord<String, String> consumerRecord) throws Exception {
		// Logging
		logger.log(
				String.format("Handling Job with Topic %s for Job ID %s", consumerRecord.topic(), consumerRecord.key()),
				PiazzaLogger.INFO);
		// Delegate by Topic
		switch (consumerRecord.topic()) {
		case JobMessageFactory.CREATE_JOB_TOPIC_NAME:
			createJobHandler.process(consumerRecord);
			break;
		case JobMessageFactory.UPDATE_JOB_TOPIC_NAME:
			updateStatusHandler.process(consumerRecord);
			break;
		case JobMessageFactory.ABORT_JOB_TOPIC_NAME:
			abortJobHandler.process(consumerRecord);
			break;
		case REPEAT_JOB_TYPE:
			repeatJobHandler.process(consumerRecord);
			break;
		}
	}
}
