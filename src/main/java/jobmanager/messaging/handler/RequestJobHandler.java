package jobmanager.messaging.handler;

import jobmanager.database.MongoAccessor;
import messaging.job.JobMessageFactory;
import model.job.Job;
import model.job.type.AbortJob;
import model.request.PiazzaJobRequest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import util.PiazzaLogger;
import util.UUIDFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Handles the Kafka topic for the requesting of a Job on the "Request-Job"
 * topic. This will relay the "Create-Job" job topic to the appropriate worker
 * components, while additionally adding in the Job metadata to the Jobs table.
 * 
 * The "Request-Job" topic will eventually deprecate the need for the
 * "Create-Job" topic; once the old Legacy API is phased out and the Dispatcher
 * is able to be dropped completely.
 * 
 * @author Patrick.Doody
 *
 */
@Component
public class RequestJobHandler {
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private MongoAccessor accessor;
	@Autowired
	private UUIDFactory uuidFactory;
	@Autowired
	private AbortJobHandler abortJobHandler;
	@Autowired
	private CreateJobHandler createJobHandler;
	@Value("${SPACE}")
	private String SPACE;

	private Producer<String, String> producer;

	/**
	 * Sets the producer for this Handler. Uses injection from the Job Messager
	 * in order to be efficient in creating only one producer, as producers are
	 * thread-safe.
	 * 
	 * @param producer
	 *            The producer.
	 */
	public void setProducer(Producer<String, String> producer) {
		this.producer = producer;
	}

	/**
	 * Processes a message on the "Request-Job" topic. This will add the Job
	 * metadata into the Jobs table, and then fire the Kafka event to the worker
	 * components to process the Job. This was previously handled by the
	 * Dispatcher.
	 * 
	 * @param consumerRecord
	 *            The Job request message.
	 */
	@Async
	public void process(ConsumerRecord<String, String> consumerRecord) {
		try {
			// Deserialize the message
			ObjectMapper mapper = new ObjectMapper();
			PiazzaJobRequest jobRequest = mapper.readValue(consumerRecord.value(), PiazzaJobRequest.class);
			String jobId = consumerRecord.key();
			process(jobRequest, jobId);
		} catch (Exception exception) {
			logger.log(String.format("Error Processing Request-Job Topic %s with key %s", consumerRecord.topic(),
					consumerRecord.key()), PiazzaLogger.ERROR);
			exception.printStackTrace();
		}
	}

	/**
	 * Processes a new Piazza Job Request. This will add the Job metadata into
	 * the Jobs table, and then fire the Kafka event to the worker components to
	 * process the Job. This was previously handled by the Dispatcher.
	 * 
	 * @param jobRequest
	 *            The Job Request
	 * @param jobId
	 *            the Job ID
	 */
	@Async
	public void process(PiazzaJobRequest jobRequest, String jobId) {
		try {
			Job job = new Job(jobRequest, jobId);
			// If the job was submitted internally, the submitter
			// wouldn't give it an ID. Assign a random ID here. (If
			// submitted via the Gateway, the Gateway will assign
			// the ID)
			if (job.getJobId().isEmpty()) {
				job.setJobId(uuidFactory.getUUID());
			}
			// The Legacy API needs the Dispatcher to handle the
			// AbortJob type. When the Legacy API becomes
			// deprecated, then this AbortJob handler will be
			// removed.
			if (job.jobType instanceof AbortJob) {
				abortJobHandler.process(jobRequest);
			} else {
				// Commit the Job metadata to the Jobs table
				createJobHandler.process(job);
				// Send the content of the actual Job under the
				// topic name of the Job type for all workers to
				// listen to.
				producer.send(JobMessageFactory.getWorkerJobCreateMessage(job, SPACE)).get();
			}
			logger.log(String.format("Relayed Job ID %s for Type %s", job.getJobId(), job.getJobType().getClass().getSimpleName()),
					PiazzaLogger.INFO);
		} catch (Exception exception) {
			logger.log(String.format("Error Processing Request-Job."), PiazzaLogger.ERROR);
			exception.printStackTrace();
		}
	}
}
