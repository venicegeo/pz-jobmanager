/**
 * Copyright 2016, RadiantBlue Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package jobmanager.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import jobmanager.database.MongoAccessor;
import jobmanager.messaging.handler.AbortJobHandler;
import jobmanager.messaging.handler.CreateJobHandler;
import jobmanager.messaging.handler.RepeatJobHandler;
import messaging.job.KafkaClientFactory;
import model.job.Job;
import model.job.type.AbortJob;
import model.job.type.RepeatJob;
import model.request.PiazzaJobRequest;
import model.response.ErrorResponse;
import model.response.JobStatusResponse;
import model.response.PiazzaResponse;
import model.status.StatusUpdate;

import org.apache.kafka.clients.producer.Producer;
import org.mongojack.DBCursor;
import org.mongojack.DBQuery;
import org.mongojack.DBSort;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import util.PiazzaLogger;
import util.UUIDFactory;

@RestController
public class JobController {
	@Value("${vcap.services.pz-kafka.credentials.host}")
	private String KAFKA_ADDRESS;

	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private UUIDFactory uuidFactory;
	@Autowired
	private MongoAccessor accessor;
	@Autowired
	private CreateJobHandler createJobHandler;
	@Autowired
	private AbortJobHandler abortJobHandler;
	@Autowired
	private RepeatJobHandler repeatJobHandler;
	@Value("${SPACE}")
	private String SPACE;

	private Producer<String, String> producer;
	private static final String DEFAULT_PAGE_SIZE = "10";
	private static final String DEFAULT_PAGE = "0";

	/**
	 * Initializing the Kafka Producer on Controller startup.
	 */
	@PostConstruct
	public void init() {
		producer = KafkaClientFactory.getProducer(KAFKA_ADDRESS.split(":")[0], KAFKA_ADDRESS.split(":")[1]);
	}

	@PreDestroy
	public void cleanup() {
		producer.close();
	}

	/**
	 * Healthcheck required for all Piazza Core Services
	 * 
	 * @return String
	 */
	@RequestMapping(value = "/", method = RequestMethod.GET)
	public String getHealthCheck() {
		return "Hello, Health Check here for pz-jobmanager.";
	}

	/**
	 * Returns the Job Status and potential Results of the specified Job ID.
	 * This is used when the Gateway needs a synchronous, non-Kafka, response
	 * for the specific status of a Job.
	 * 
	 * @param jobId
	 *            The Job ID.
	 * @return The StatusResponse object of the Job. This will, at the very
	 *         least, involved the readiness of the Job. If not ready, the
	 *         available Status and Progress of the Job will be included in this
	 *         response object. If the job is ready, then this Response will
	 *         contain an Object reference to the output produced by the Job.
	 */
	@RequestMapping(value = "/job/{jobId}", method = RequestMethod.GET)
	public PiazzaResponse getJobStatus(@PathVariable(value = "jobId") String jobId) {
		try {
			if (jobId.isEmpty()) {
				throw new Exception("No Job ID specified.");
			}
			// Query for the Job ID
			Job job = accessor.getJobById(jobId);
			// If no Job was found.
			if (job == null) {
				logger.log(String.format("Job not found for requested ID %s", jobId), PiazzaLogger.WARNING);
				return new ErrorResponse(jobId, "Job Not Found.", "Job Manager");
			}
			// Return Job Status
			logger.log(String.format("Returning Job Status for %s", jobId), PiazzaLogger.INFO);
			return new JobStatusResponse(job);
		} catch (Exception exception) {
			logger.log(String.format("Error fetching a Job %s: %s", jobId, exception.getMessage()), PiazzaLogger.ERROR);
			return new ErrorResponse(jobId, "Error fetching Job: " + exception.getMessage(), "Job Manager");
		}
	}

	/**
	 * Acts like the "Create-Job" kafka topic handler. This will insert Job
	 * information into the Jobs table and set the state to "Submitted."
	 * 
	 * <p>
	 * This will be deprecated once the legacy API is removed; at which point
	 * this endpoint can be deleted.
	 * </p>
	 * 
	 * @param job
	 *            The job information to add to the table.
	 * @return OK if added. Error if not.
	 */
	@RequestMapping(value = "/createJob", method = RequestMethod.POST)
	public PiazzaResponse createJob(@RequestBody Job job) {
		try {
			createJobHandler.process(job);
			return null;
		} catch (Exception exception) {
			String error = String.format("Error committing Job %s to the database: %s", job.getJobId(),
					exception.getMessage());
			logger.log(error, PiazzaLogger.ERROR);
			return new ErrorResponse(job.getJobId(), error, "Job Manager");
		}
	}

	/**
	 * Aborts a currently running Piazza Job.
	 * 
	 * @param request
	 *            The request, detailing the AbortJob type and the user who has
	 *            requested this action.
	 * @return null response if successful. ErrorResponse containing appropriate
	 *         error details on exception.
	 */
	@RequestMapping(value = "/abort", method = RequestMethod.POST)
	public PiazzaResponse abortJob(@RequestBody PiazzaJobRequest request) {
		try {
			// Abort the Job in the Jobs table.
			abortJobHandler.process(request);
			// Log the successful Cancellation
			logger.log(String.format("Successfully cancelled Job %s by User %s",
					((AbortJob) request.jobType).getJobId(), request.userName), PiazzaLogger.INFO);
			// Return OK
			return null;
		} catch (Exception exception) {
			logger.log(String.format("Error Cancelling Job: ", exception.getMessage()), PiazzaLogger.ERROR);
			return new ErrorResponse(null, "Error Cancelling Job: " + exception.getMessage(), "Job Manager");
		}
	}

	/**
	 * Repeats an existing Job within Piazza.
	 * 
	 * @param request
	 *            The request, detailing the RepeatJob type and the user who has
	 *            submitted this request.
	 * @return The response containing the Job ID if successful. Appropriate
	 *         error details returned on exception.
	 */
	@RequestMapping(value = "/repeat", method = RequestMethod.POST)
	public PiazzaResponse repeatJob(@RequestBody PiazzaJobRequest request) {
		try {
			// Repeat the Job
			String newJobId = repeatJobHandler.process(request);
			// Log the successful Repetition of the Job
			logger.log(String.format("Successfully created a Repeat Job under ID %s for original Job ID %s by user %s",
					newJobId, ((RepeatJob) request.jobType).getJobId(), request.userName), PiazzaLogger.INFO);
			// Return the Job ID
			return new PiazzaResponse(newJobId);
		} catch (Exception exception) {
			logger.log(String.format("Error Repeating Job: ", exception.getMessage()), PiazzaLogger.ERROR);
			return new ErrorResponse(null, "Error Repeating Job: " + exception.getMessage(), "Job Manager");
		}
	}

	/**
	 * Returns Jobs currently held by the Piazza Job table.
	 * 
	 * This is intended to be used by the Swiss-Army-Knife (SAK) administration
	 * application for reporting the status of this Job Manager component. It is
	 * not used in normal function of the Job Manager.
	 * 
	 * @param page
	 *            The start page
	 * @param pageSize
	 *            The number of results per page
	 * @return The List of all Jobs in the system.
	 */
	@RequestMapping(value = "/job", method = RequestMethod.GET)
	public List<Job> getJobs(@RequestParam(value = "page", required = false, defaultValue = DEFAULT_PAGE) String page,
			@RequestParam(value = "per_page", required = false, defaultValue = DEFAULT_PAGE_SIZE) String pageSize,
			@RequestParam(value = "order", required = false) String order) {
		return accessor.getJobs(Integer.parseInt(page), Integer.parseInt(pageSize), order);
	}

	/**
	 * Returns the Number of Jobs in the piazza system.
	 * 
	 * This is intended to be used by the Swiss-Army-Knife (SAK) administration
	 * application for reporting the status of this Job Manager component. It is
	 * not used in normal function of the Job Manager.
	 * 
	 * @return Number of Jobs in the system.
	 */
	@RequestMapping(value = "/job/count", method = RequestMethod.GET)
	public long getJobCount() {
		return accessor.getJobCollection().count();
	}

	/**
	 * Gets all Job Status types that can be queried for by the system.
	 * 
	 * This is intended to be used by the Swiss-Army-Knife (SAK) administration
	 * application for reporting the status of this Job Manager component. It is
	 * not used in normal function of the Job Manager.
	 * 
	 * @return List of Job Status types that can be queried for in the system.
	 */
	@RequestMapping(value = "/job/status", method = RequestMethod.GET)
	public List<String> getStatuses() {
		List<String> statuses = new ArrayList<String>();
		statuses.add(StatusUpdate.STATUS_CANCELLED);
		statuses.add(StatusUpdate.STATUS_ERROR);
		statuses.add(StatusUpdate.STATUS_FAIL);
		statuses.add(StatusUpdate.STATUS_PENDING);
		statuses.add(StatusUpdate.STATUS_RUNNING);
		statuses.add(StatusUpdate.STATUS_SUBMITTED);
		statuses.add(StatusUpdate.STATUS_SUCCESS);
		return statuses;
	}

	/**
	 * Gets the count for all Jobs of the specified status.
	 * 
	 * This is intended to be used by the Swiss-Army-Knife (SAK) administration
	 * application for reporting the status of this Job Manager component. It is
	 * not used in normal function of the Job Manager.
	 * 
	 * @return List of Jobs that match the specified status.
	 */
	@RequestMapping(value = "/job/status/{status}/count", method = RequestMethod.GET)
	public int getStatusCount(@PathVariable(value = "status") String status) {
		return accessor.getJobCollection().find(DBQuery.is("status", status)).count();
	}

	/**
	 * Gets all Jobs, filtered by their Status. This follows the same pagination
	 * rules as the getJobs() request.
	 * 
	 * This is intended to be used by the Swiss-Army-Knife (SAK) administration
	 * application for reporting the status of this Job Manager component. It is
	 * not used in normal function of the Job Manager.
	 * 
	 * @param page
	 *            The start page
	 * @param pageSize
	 *            The number of results per page
	 * @return The list of all Jobs that match the specified Status
	 */
	@RequestMapping(value = "/job/status/{status}", method = RequestMethod.GET)
	public List<Job> getJobsByStatus(
			@RequestParam(value = "page", required = false, defaultValue = DEFAULT_PAGE) String page,
			@RequestParam(value = "pageSize", required = false, defaultValue = DEFAULT_PAGE_SIZE) String pageSize,
			@PathVariable(value = "status") String status, @RequestParam(value = "order", required = false) String order) {
		return accessor.getJobsForStatus(Integer.parseInt(page), Integer.parseInt(pageSize), order, status);
	}

	/**
	 * Gets the list of all Jobs for a certain user ID.
	 * 
	 * @param page
	 *            The start page
	 * @param pageSize
	 *            The number of results per page
	 * @param userName
	 *            The userName of the user to query Jobs for.
	 * @return
	 */
	@RequestMapping(value = "/job/userName/{userName}", method = RequestMethod.GET)
	public List<Job> getJobsByUserName(
			@RequestParam(value = "page", required = false, defaultValue = DEFAULT_PAGE) String page,
			@RequestParam(value = "pageSize", required = false, defaultValue = DEFAULT_PAGE_SIZE) String pageSize,
			@PathVariable(value = "userName") String userName,
			@RequestParam(value = "order", required = false) String order) {
		return accessor.getJobsForUser(Integer.parseInt(page), Integer.parseInt(pageSize), order, userName);
	}

	/**
	 * Returns administrative statistics for this component.
	 * 
	 * @return Component information
	 */
	@RequestMapping(value = "/admin/stats", method = RequestMethod.GET)
	public ResponseEntity<Map<String, Object>> getAdminStats() {
		Map<String, Object> stats = new HashMap<String, Object>();
		// Add information related to the Jobs in the system
		stats.put("total", accessor.getJobCollection().getCount());
		stats.put("success", getStatusCount(StatusUpdate.STATUS_SUCCESS));
		stats.put("error", getStatusCount(StatusUpdate.STATUS_ERROR));
		stats.put("fail", getStatusCount(StatusUpdate.STATUS_FAIL));
		stats.put("running", getStatusCount(StatusUpdate.STATUS_RUNNING));
		stats.put("pending", getStatusCount(StatusUpdate.STATUS_PENDING));
		stats.put("submitted", getStatusCount(StatusUpdate.STATUS_SUBMITTED));
		stats.put("cancelled", getStatusCount(StatusUpdate.STATUS_CANCELLED));

		return new ResponseEntity<Map<String, Object>>(stats, HttpStatus.OK);
	}

	/**
	 * Drops the Mongo collections. This is for internal development use only.
	 * We should probably remove this in the future. Don't use this.
	 */
	@RequestMapping(value = "/drop")
	public String dropJobTables(@RequestParam(value = "serious", required = false) Boolean serious) {
		if ((serious != null) && (serious.booleanValue())) {
			accessor.getJobCollection().drop();
			return "Jobs dropped.";
		} else {
			return "You're not serious.";
		}
	}
}