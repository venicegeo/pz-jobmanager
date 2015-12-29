package main.java.jobmanager.messaging;

import java.util.Properties;

import javax.annotation.PostConstruct;

import main.java.jobmanager.database.MongoAccessor;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
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

	public JobMessager() {

	}

	@PostConstruct
	public void initialize() {
		// Initialize the Consumer and Producer
		initializeProducer();
		initializeConsumer();
		// Start the runner that will relay Job Creation topics.
		Thread createJobThread = new Thread(new CreateJobRunner(consumer, accessor));
		createJobThread.start();
	}

	/**
	 * Initializing the Kafka Producer that will relay messages.
	 * 
	 * TODO: Config please
	 */
	private void initializeProducer() {
		// Initialize the Kafka Producer
		Properties props = new Properties();
		props.put("bootstrap.servers", String.format("%s:%s", KAFKA_HOST, KAFKA_PORT));
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
		props.put("bootstrap.servers", String.format("%s:%s", KAFKA_HOST, KAFKA_PORT));
		props.put("group.id", KAFKA_GROUP);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<String, String>(props);
	}
}
