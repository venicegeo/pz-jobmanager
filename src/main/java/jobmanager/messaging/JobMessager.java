package main.java.jobmanager.messaging;

import javax.annotation.PostConstruct;

import main.java.jobmanager.database.MongoAccessor;
import messaging.job.KafkaClientFactory;

import org.apache.kafka.clients.consumer.Consumer;
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
		producer = KafkaClientFactory.getProducer(KAFKA_HOST, KAFKA_PORT);
		consumer = KafkaClientFactory.getConsumer(KAFKA_HOST, KAFKA_PORT, KAFKA_GROUP);
		// Start the runner that will relay Job Creation topics.
		Thread createJobThread = new Thread(new CreateJobRunner(consumer, accessor));
		createJobThread.start();
	}
}
