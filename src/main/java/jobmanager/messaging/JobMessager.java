package main.java.jobmanager.messaging;

import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

/**
 * Consumes Kafka messages related to Jobs.
 * 
 * @author Patrick.Doody
 * 
 */
public class JobMessager {
	private Producer<String, String> producer;
	private Consumer<String, String> consumer;

	/**
	 * 
	 */
	public JobMessager() {
		initializeProducer();
		initializeConsumer();
	}

	/**
	 * 
	 */
	public void initialize() {
		// Start the runner that will relay Job Creation topics.
		CreateJobRunner createJobRunner = new CreateJobRunner(consumer);
		createJobRunner.run();
	}

	/**
	 * Initializing the Kafka Producer that will relay messages.
	 * 
	 * TODO: Config please
	 */
	private void initializeProducer() {
		// Initialize the Kafka Producer
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<String, String>(props);
	}

	/**
	 * Initializing the Kafka Consumer that will consume relayable messages.
	 * 
	 * TODO: Config please
	 */
	private void initializeConsumer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "TEST-GROUP");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<String, String>(props);
	}
}
