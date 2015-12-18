package main.java.jobmanager.messaging;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import model.job.PiazzaJob;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.mongojack.JacksonDBCollection;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CreateJobRunner implements Runnable {
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private Consumer<String, String> consumer;
	private JacksonDBCollection<PiazzaJob, String> collection;

	/**
	 * Thread runner that will consume incoming Job Creation Kafka messages and
	 * create corresponding entries in the Jobs table.
	 * 
	 * @param consumer
	 * @param collection
	 */
	public CreateJobRunner(Consumer<String, String> consumer, JacksonDBCollection<PiazzaJob, String> collection) {
		this.consumer = consumer;
		this.collection = collection;
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
						PiazzaJob job = mapper.readValue(consumerRecord.value(), PiazzaJob.class);
						collection.insert(job);
					} catch (Exception exception) {
						System.out.println("Error committing Job: " + exception.getMessage());
					}

				}
			}
		} catch (WakeupException e) {
			// Ignore exception if closing
			if (!closed.get()) {
				throw e;
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