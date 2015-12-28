package main.java.jobmanager.messaging;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import main.java.jobmanager.database.MongoAccessor;
import model.job.Job;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CreateJobRunner implements Runnable {
	private MongoAccessor accessor; 
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private Consumer<String, String> consumer;

	/**
	 * Thread runner that will consume incoming Job Creation Kafka messages and
	 * create corresponding entries in the Jobs table.
	 * 
	 * @param consumer
	 * @param collection
	 */
	public CreateJobRunner(Consumer<String, String> consumer, MongoAccessor accessor) {
		this.consumer = consumer;
		this.accessor = accessor;
	}

	public void run() {
		try {
			consumer.subscribe(Arrays.asList("Create-Job"));
			while (!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
				// Handle new Messages on this topic.
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					// Logging
					System.out.println("Indexing new job with topic " + consumerRecord.topic() + " and key "
							+ consumerRecord.key() + " with value " + consumerRecord.value());

					// Inserting Job Information into the Job Table
					try {
						ObjectMapper mapper = new ObjectMapper();
						Job job = mapper.readValue(consumerRecord.value(), Job.class);
						accessor.getJobCollection().insert(job);
					} catch (Exception exception) {
						System.out.println("Error committing Job: " + exception.getMessage());
						exception.printStackTrace();
					}
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

	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}
}