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
import java.util.List;

import jobmanager.database.MongoAccessor;
import model.job.Job;
import model.response.ErrorResponse;
import model.response.JobStatusResponse;
import model.response.PiazzaResponse;
import model.status.StatusUpdate;

import org.mongojack.DBQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import util.PiazzaLogger;

@RestController
public class JobController {
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private MongoAccessor accessor;
	private static final String DEFAULT_PAGE_SIZE = "10";
	private static final String DEFAULT_PAGE = "0";

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
			@RequestParam(value = "pageSize", required = false, defaultValue = DEFAULT_PAGE_SIZE) String pageSize) {
		return accessor.getJobCollection().find().skip(Integer.parseInt(page) * Integer.parseInt(pageSize))
				.limit(Integer.parseInt(pageSize)).toArray();
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
		statuses.add(StatusUpdate.STATUS_COMPLETE);
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
			@PathVariable(value = "status") String status) {
		return accessor.getJobCollection().find(DBQuery.is("status", status))
				.skip(Integer.parseInt(page) * Integer.parseInt(pageSize)).limit(Integer.parseInt(pageSize)).toArray();
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