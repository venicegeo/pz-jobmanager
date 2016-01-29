package jobmanager.messaging.handler;

import jobmanager.database.MongoAccessor;
import model.job.type.AbortJob;
import model.status.StatusUpdate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mongojack.DBQuery;
import org.mongojack.DBUpdate;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Handles the request for Aborting a Job by updating the Job table with the new
 * Status.
 * 
 * @author Patrick.Doody
 * 
 */
public class AbortJobHandler {
	private MongoAccessor accessor;

	public AbortJobHandler(MongoAccessor accessor) {
		this.accessor = accessor;
	}

	public void process(ConsumerRecord<String, String> consumerRecord) {
		// Changing the Status in the Job Table to Aborted
		try {
			ObjectMapper mapper = new ObjectMapper();
			AbortJob job = mapper.readValue(consumerRecord.value(), AbortJob.class);
			// Update the status of the Job
			accessor.getJobCollection().update(DBQuery.is("jobId", job.getJobId()),
					DBUpdate.set("status", StatusUpdate.STATUS_CANCELLED));
		} catch (Exception exception) {
			System.out.println("Error setting aborted Job status in Collection: " + exception.getMessage());
			exception.printStackTrace();
		}
	}
}
