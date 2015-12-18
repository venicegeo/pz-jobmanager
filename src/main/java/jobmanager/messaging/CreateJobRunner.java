package main.java.jobmanager.messaging;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

public class CreateJobRunner implements Runnable {
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private Consumer<String, String> consumer;

	/**
	 * 
	 * @param consumer
	 */
	public CreateJobRunner(Consumer<String, String> consumer) {
		this.consumer = consumer;
	}

	public void run() {
		try {
			consumer.subscribe(Arrays.asList("Create-Job"));
			while (!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
				// Handle new Messages on this topic.
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					System.out.println("Job Manager indexing new job with topic " + consumerRecord.topic()
							+ " and key " + consumerRecord.key());
					// Insert the Job Information into the Job Table
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
