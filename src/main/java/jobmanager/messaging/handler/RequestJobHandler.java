package jobmanager.messaging.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import jobmanager.database.MongoAccessor;
import messaging.job.JobMessageFactory;
import model.job.Job;
import model.request.PiazzaJobRequest;
import util.PiazzaLogger;
import util.UUIDFactory;

/**
 * Handles the Kafka topic for the requesting of a Job on the "Request-Job" topic. This will relay the "Create-Job" job
 * topic to the appropriate worker components, while additionally adding in the Job metadata to the Jobs table.
 * 
 * @author Patrick.Doody
 *
 */
@Component
public class RequestJobHandler {
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private UUIDFactory uuidFactory;
	@Autowired
	private MongoAccessor accessor;
	@Value("${SPACE}")
	private String SPACE;

	private Producer<String, String> producer;
	ObjectMapper mapper = new ObjectMapper();

	/**
	 * Sets the producer for this Handler. Uses injection from the Job Messager in order to be efficient in creating
	 * only one producer, as producers are thread-safe.
	 * 
	 * @param producer
	 *            The producer.
	 */
	public void setProducer(Producer<String, String> producer) {
		this.producer = producer;
	}

	/**
	 * Processes a message on the "Request-Job" topic. This will add the Job metadata into the Jobs table, and then fire
	 * the Kafka event to the worker components to process the Job.
	 * 
	 * @param consumerRecord
	 *            The Job request message.
	 */
	@Async
	public void process(ConsumerRecord<String, String> consumerRecord) {
		try {
			// Deserialize the message
			PiazzaJobRequest jobRequest = mapper.readValue(consumerRecord.value(), PiazzaJobRequest.class);
			String jobId = consumerRecord.key();
			process(jobRequest, jobId);
		} catch (Exception exception) {
			logger.log(String.format("Error Processing Request-Job Topic %s with key %s with Error: %s", consumerRecord.topic(),
					consumerRecord.key(), exception.getMessage()), PiazzaLogger.ERROR);
		}
	}

	/**
	 * Processes a new Piazza Job Request. This will add the Job metadata into the Jobs table, and then fire the Kafka
	 * event to the worker components to process the Job.
	 * 
	 * @param jobRequest
	 *            The Job Request
	 * @param jobId
	 *            the Job Id
	 */
	public void process(PiazzaJobRequest jobRequest, String jobId) {
		try {
			Job job = new Job(jobRequest, jobId);
			// If the job was submitted internally, the submitter
			// wouldn't give it an Id. Assign a random Id here. (If
			// submitted via the Gateway, the Gateway will assign
			// the Id)
			if (job.getJobId().isEmpty()) {
				job.setJobId(uuidFactory.getUUID());
			}
			// Commit the Job metadata to the Jobs table
			accessor.getJobCollection().insert(job);
			// Send the content of the actual Job under the
			// topic name of the Job type for all workers to
			// listen to.
			producer.send(JobMessageFactory.getWorkerJobCreateMessage(job, SPACE));
			logger.log(String.format("Relayed Job Id %s for Type %s", job.getJobId(), job.getJobType().getClass().getSimpleName()),
					PiazzaLogger.INFO);
		} catch (Exception exception) {
			logger.log(String.format("Error Processing Request-Job with error %s", exception.getMessage()), PiazzaLogger.ERROR);
		}
	}
}
