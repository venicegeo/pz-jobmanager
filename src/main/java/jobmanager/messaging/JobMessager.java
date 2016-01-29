package jobmanager.messaging;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import jobmanager.database.MongoAccessor;
import jobmanager.messaging.handler.AbortJobHandler;
import jobmanager.messaging.handler.CreateJobHandler;
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

@Component
public class JobMessager {
	@Autowired
	private MongoAccessor accessor;
	@Value("${kafka.host}")
	private String KAFKA_HOST;
	@Value("${kafka.port}")
	private String KAFKA_PORT;
	@Value("${kafka.group}")
	private String KAFKA_GROUP;
	private Producer<String, String> producer;
	private Consumer<String, String> consumer;
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private AbortJobHandler abortJobHandler;
	private CreateJobHandler createJobHandler;
	private UpdateStatusHandler updateStatusHandler;

	public JobMessager() {
	}

	@PostConstruct
	public void initialize() {
		// Initialize the Consumer and Producer
		producer = KafkaClientFactory.getProducer(KAFKA_HOST, KAFKA_PORT);
		consumer = KafkaClientFactory.getConsumer(KAFKA_HOST, KAFKA_PORT, KAFKA_GROUP);
		// Initialize Handlers
		abortJobHandler = new AbortJobHandler(accessor);
		createJobHandler = new CreateJobHandler(accessor);
		updateStatusHandler = new UpdateStatusHandler(accessor);
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
					JobMessageFactory.UPDATE_JOB_TOPIC_NAME, JobMessageFactory.ABORT_JOB_TOPIC_NAME));
			// Continuously poll for these topics
			while (!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
				// Handle new Messages on this topic.
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					// Logging
					System.out.println("Received job with topic " + consumerRecord.topic() + " and key "
							+ consumerRecord.key() + " with value " + consumerRecord.value());
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
					}
					// Send a response back to the Gateway, which will be
					// listening for this Topic, to signal that the Job was
					// processed. TODO: Meaningful values for these 3
					// parameters?
					producer.send(JobMessageFactory.getJobReturnMessage(consumerRecord.key(), consumerRecord.key(),
							consumerRecord.key()));
				}
			}
		} catch (WakeupException exception) {
			// Ignore exception if closing
			if (!closed.get()) {
				throw exception;
			}
		} finally {
			consumer.close();
		}
	}
}
