package main.java.jobmanager.messaging.handler;

import main.java.jobmanager.database.MongoAccessor;
import model.status.StatusUpdate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mongojack.DBQuery;
import org.mongojack.DBUpdate;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Handles the request for Updating the Status of a Job in the Job Table
 * 
 * @author Patrick.Doody
 * 
 */
public class UpdateStatusHandler {
	private MongoAccessor accessor;

	public UpdateStatusHandler(MongoAccessor accessor) {
		this.accessor = accessor;
	}

	public void process(ConsumerRecord<String, String> consumerRecord) {
		// Changing the Status in the Job Table
		try {
			ObjectMapper mapper = new ObjectMapper();
			StatusUpdate statusUpdate = mapper.readValue(consumerRecord.value(), StatusUpdate.class);
			// Update the Job Status, if specified
			if (!statusUpdate.getStatus().isEmpty()) {
				accessor.getJobCollection().update(DBQuery.is("jobId", consumerRecord.key()),
						DBUpdate.set("status", statusUpdate.getStatus()));
			}
			// Update the Job progress, if specified
			if (statusUpdate.getJobProgress() != null) {
				accessor.getJobCollection().update(DBQuery.is("jobId", consumerRecord.key()),
						DBUpdate.set("progress", statusUpdate.getJobProgress()));
			}
		} catch (Exception exception) {
			System.out.println("Error updating Job status in Collection: " + exception.getMessage());
			exception.printStackTrace();
		}
	}
}
