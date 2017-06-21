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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jobmanager.messaging.handler.RepeatJobHandler;
import jobmanager.messaging.handler.RequestJobHandler;
import jobmanager.messaging.handler.UpdateStatusHandler;
import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;
import model.logger.AuditElement;
import model.logger.Severity;
import util.PiazzaLogger;

/**
 * Interacts with the Jobs Collection in the Mongo database based on Kafka messages received.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class JobMessager {
	@Autowired
	private PiazzaLogger logger;

	@Autowired
	private UpdateStatusHandler updateStatusHandler;
	@Autowired
	private RepeatJobHandler repeatJobHandler;
	@Autowired
	private RequestJobHandler requestJobHandler;

	@Value("${vcap.services.pz-kafka.credentials.host}")
	private String KAFKA_HOSTS;
	@Value("${SPACE}")
	private String SPACE;
	@Value("#{'${kafka.group}' + '-' + '${SPACE}'}")
	private String KAFKA_GROUP;
	private String UPDATE_JOB_TOPIC_NAME;
	private String REQUEST_JOB_TOPIC_NAME;
	private Producer<String, String> producer;
	private Consumer<String, String> consumer;
	private final AtomicBoolean closed = new AtomicBoolean(false);

	private static final Logger LOG = LoggerFactory.getLogger(JobMessager.class);

	public JobMessager() {
		// Expected for Component instantiation
	}

	@PostConstruct
	public void initialize() {
		// Initialize the topics
		UPDATE_JOB_TOPIC_NAME = String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, SPACE);
		REQUEST_JOB_TOPIC_NAME = String.format("%s-%s", "Request-Job", SPACE);
		// Initialize the Consumer and Producer
		consumer = KafkaClientFactory.getConsumer(KAFKA_HOSTS, KAFKA_GROUP);
		producer = KafkaClientFactory.getProducer(KAFKA_HOSTS);
		// Share the producer
		repeatJobHandler.setProducer(producer);
		requestJobHandler.setProducer(producer);
		// Immediately Poll on a new thread
		Thread pollThread = new Thread() {
			@Override
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
			List<String> topics = Arrays.asList(UPDATE_JOB_TOPIC_NAME, REQUEST_JOB_TOPIC_NAME);
			consumer.subscribe(topics);
			// Log that we are listening
			logger.log(String.format("Listening to Kafka at %s and subscribed to topics %s", KAFKA_HOSTS, topics.toString()),
					Severity.INFORMATIONAL);
			// Continuously poll for these topics
			while (!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
				// Handle new Messages on this topic.
				handleNewMessage(consumerRecords);
			}
		} catch (WakeupException exception) {
			logger.log(String.format("Job Listener Thread forcefully shut: %s", exception.getMessage()), Severity.ERROR);
			// Ignore exception if closing
			if (!closed.get()) {
				throw exception;
			}
		} finally {
			consumer.close();
		}
	}

	private void handleNewMessage(final ConsumerRecords<String,String> consumerRecords) {
		for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
			try {
				processMessage(consumerRecord);
			} catch (Exception exception) {
				String error = String.format("Error processing Job with Key %s under Topic %s. Error: %s", consumerRecord.key(),
						consumerRecord.topic(), exception.getMessage());
				LOG.error(error, exception);
				logger.log(error, Severity.ERROR, new AuditElement("jobmanager", "errorProcessingKafkaJob", consumerRecord.key()));
			}
		}
	}
	
	/**
	 * Stops polling.
	 */
	public void stopPolling() {
		this.closed.set(true);
	}

	/**
	 * Processes an incoming Kafka Message. This will pass it off to the appropriate handler that will make the
	 * necessary changes to the Jobs Table.
	 * 
	 * @param consumerRecord
	 *            The message to process.
	 */
	public void processMessage(ConsumerRecord<String, String> consumerRecord) {
		// Delegate by Topic
		if (consumerRecord.topic().equalsIgnoreCase(UPDATE_JOB_TOPIC_NAME)) {
			updateStatusHandler.process(consumerRecord);
		} else if (consumerRecord.topic().equalsIgnoreCase(REQUEST_JOB_TOPIC_NAME)) {
			requestJobHandler.process(consumerRecord);
		} else {
			logger.log(String.format("Received a Topic that could not be processed: %s", consumerRecord.topic()), Severity.WARNING);
		}
	}
}
