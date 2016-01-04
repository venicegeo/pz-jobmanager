package main.java.jobmanager.messaging.handler;

import main.java.jobmanager.database.MongoAccessor;
import model.job.Job;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Creates corresponding entries in the Jobs table for the Kafka Message
 * 
 * @author Patrick.Doody
 * 
 */
public class CreateJobHandler {
	private MongoAccessor accessor;

	public CreateJobHandler(MongoAccessor accessor) {
		this.accessor = accessor;
	}

	public void process(ConsumerRecord<String, String> consumerRecord) {
		// Inserting Job Information into the Job Table
		try {
			ObjectMapper mapper = new ObjectMapper();
			Job job = mapper.readValue(consumerRecord.value(), Job.class);
			accessor.getJobCollection().insert(job);
		} catch (Exception exception) {
			System.out.println("Error adding new Job to Collection: " + exception.getMessage());
			exception.printStackTrace();
		}
	}
}