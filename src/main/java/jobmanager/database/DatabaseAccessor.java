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
package jobmanager.database;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.venice.piazza.common.hibernate.dao.job.JobDao;
import org.venice.piazza.common.hibernate.entity.JobEntity;

import model.job.Job;
import model.job.JobProgress;
import model.response.JobListResponse;
import model.status.StatusUpdate;

/**
 * Helper class to interact with and access the Database
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class DatabaseAccessor {

	// @Value("${vcap.services.pz-mongodb.credentials.uri}")
	// private String DATABASE_URI;
	// @Value("${vcap.services.pz-mongodb.credentials.database}")
	// private String DATABASE_NAME;
	// @Value("${vcap.services.pz-mongodb.credentials.host}")
	// private String DATABASE_HOST;
	// @Value("${vcap.services.pz-mongodb.credentials.port}")
	// private int DATABASE_PORT;
	// @Value("${vcap.services.pz-mongodb.credentials.username:}")
	// private String DATABASE_USERNAME;
	// @Value("${vcap.services.pz-mongodb.credentials.password:}")
	// private String DATABASE_CREDENTIAL;
	// @Value("${mongo.db.collection.name}")
	// private String JOB_COLLECTION_NAME;
	// @Value("${mongo.thread.multiplier}")
	// private int mongoThreadMultiplier;
	// private MongoClient mongoClient;

	@Autowired
	private JobDao jobDao;

	private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseAccessor.class);
	private static final String STATUS = "status";
	private static final String JOBID = "jobId";

	public DatabaseAccessor() {
		// Expected for Component instantiation
	}

	/**
	 * Gets the total number of Jobs in the database
	 * 
	 * @return Number of jobs in the DB
	 */
	public long getJobsCount() {
		return jobDao.count();
	}

	/**
	 * Gets the number of Jobs for the specific status
	 * 
	 * @param status
	 *            The Status
	 * @return The number of Jobs for that status
	 */
	public Long getJobStatusCount(String status) {
		return jobDao.countJobByStatus(status);
	}

	/**
	 * Returns a Job that matches the specified Id.
	 * 
	 * @param jobId
	 *            Job Id
	 * @return The Job with the specified Id
	 * @throws InterruptedException
	 */
	public Job getJobById(String jobId) throws ResourceAccessException, InterruptedException {
		JobEntity jobEntity = jobDao.getJobByJobId(jobId);
		return jobEntity != null ? jobEntity.getJob() : null;
	}

	/**
	 * Gets a list of Jobs from the database.
	 * 
	 * @param page
	 *            the page number
	 * @param perPage
	 *            the number of results per page
	 * @param order
	 *            "ascending" or "descending"
	 * @param status
	 *            The status of the Job
	 * @param userName
	 *            The username who submitted the Job
	 * @return The list of jobs
	 */
	public JobListResponse getJobs(int page, int perPage, String order, String sortBy, String status, String userName) {
		return null;
		// // Construct the query based on the user parameters.
		// Query query = DBQuery.empty();
		// if ((userName != null) && (userName.isEmpty() == false)) {
		// query.and(DBQuery.is("createdBy", userName));
		// }
		// if ((status != null) && (status.isEmpty() == false)) {
		// query.and(DBQuery.is(STATUS, status));
		// }
		//
		// // Execute the query
		// DBCursor<Job> cursor = getJobCollection().find(query);
		//
		// // Sort and order the Results
		// if ("asc".equalsIgnoreCase(order)) {
		// cursor = cursor.sort(DBSort.asc(sortBy));
		// } else if ("desc".equalsIgnoreCase(order)) {
		// cursor = cursor.sort(DBSort.desc(sortBy));
		// }
		//
		// // Get the total count
		// Integer size = Integer.valueOf(cursor.size());
		//
		// // Paginate the results
		// List<Job> jobs = cursor.skip(page * perPage).limit(perPage).toArray();
		//
		// // Attach pagination information
		// Pagination pagination = new Pagination(size, page, perPage, sortBy, order);
		//
		// // Create the Response and send back
		// return new JobListResponse(jobs, pagination);
	}

	/**
	 * Updates the status of a Job.
	 * 
	 * @param jobId
	 *            The Job Id
	 * @param status
	 *            The Status String of the Job
	 */
	public void updateJobStatus(String jobId, String status) {
		// getJobCollection().update(DBQuery.is(JOBID, jobId), DBUpdate.set(STATUS, status));
	}

	/**
	 * Updates the Progress of a Job
	 * 
	 * @param jobId
	 *            The Job Id to update
	 * @param progress
	 *            The progres to set
	 */
	public void updateJobProgress(String jobId, JobProgress progress) {
		// getJobCollection().update(DBQuery.is(JOBID, jobId), DBUpdate.set("progress", progress));
	}

	/**
	 * Updates the Status of a Job. This will update the result, progress, and status of the Job. This method will
	 * update in a single write to the database.
	 * 
	 * @param jobId
	 *            The Id of the Job whose status to update
	 * @param statusUpdate
	 *            The Status Update information
	 */
	public void updateJobStatus(String jobId, StatusUpdate statusUpdate) throws ResourceAccessException, InterruptedException {
		// // Determine if the Result is part of the status. If so, then this will be an entire delete/re-entry of the
		// Job
		// // object into the database.
		// if (statusUpdate.getResult() != null) {
		// updateJobStatusWithResult(jobId, statusUpdate);
		// } else {
		// // If the Result is not part of the Status, then we can update the existing Job fields in a single commit.
		// // Form the DBUpdate, which will modify the fields
		// Builder update = new Builder();
		// if (statusUpdate.getProgress() != null) {
		// update.set("progress", statusUpdate.getProgress());
		// }
		// if (statusUpdate.getStatus().isEmpty() == false) {
		// update.set(STATUS, statusUpdate.getStatus());
		// }
		// // Form the query to update the job matching the ID. Do not update if the Job Status is in a finished state.
		// Query query = DBQuery.is(JOBID, jobId).and(DBQuery.notEquals(STATUS,
		// StatusUpdate.STATUS_CANCELLED).and(DBQuery
		// .notEquals(STATUS, StatusUpdate.STATUS_ERROR)
		// .and(DBQuery.notEquals(STATUS, StatusUpdate.STATUS_FAIL).and(DBQuery.notEquals(STATUS,
		// StatusUpdate.STATUS_SUCCESS)))));
		// // Commit
		// getJobCollection().update(query, update);
		// }
	}

	/**
	 * Updates the Job with the Status Update; a Status that contains a Result object.
	 * 
	 * It is important to note that we are not doing an update of the Mongo Resource here, as one would expect. This is
	 * due to a bug in MongoJack, documented here: https://github.com/mongojack/mongojack/issues/101; that explains how
	 * updating of MongoJack collections with polymorphic objects currently only serializes the fields found in the
	 * parent class or interface, and all child fields are ignored.
	 * 
	 * This is important for us because the Results of a Job are polymorphic (specifically, the ResultType interface)
	 * and thus are not getting properly serialized as a result of this bug. This bug exists in all versions of
	 * MongoJack and is still OPEN in GitHub issues.
	 * 
	 * Due to this issue, we are updating the Job properties in a Job object, and then deleting that object from the
	 * database and immediately committing the new Job with the updates. The above-mentioned bug only affects updates,
	 * so the work-around here is avoiding updates by creating a new object in the database. This is functionally
	 * acceptable because we make no use of MongoDB's primary key - our key is based on the JobId property, which is
	 * maintained throughout the transaction.
	 * 
	 * @param jobId
	 *            The Job Id to update
	 * @param statusUpdate
	 *            The Status Update with the Result (and any other Status information)
	 */
	private synchronized void updateJobStatusWithResult(String jobId, StatusUpdate statusUpdate)
			throws ResourceAccessException, InterruptedException {
		// // Get the existing Job and all of its properties
		// Job job = getJobById(jobId);
		// // Remove existing Job
		// removeJob(jobId);
		// // Update the Job object with the Status information.
		// if (statusUpdate.getStatus().isEmpty() == false) {
		// job.setStatus(statusUpdate.getStatus());
		// }
		// if (statusUpdate.getProgress() != null) {
		// job.setProgress(statusUpdate.getProgress());
		// }
		// // Set the Result
		// job.setResult(statusUpdate.getResult());
		// // Re-add the Job to the database.
		// addJob(job);
	}

	/**
	 * Deletes a Job entry.
	 * 
	 * @param jobId
	 *            The Id of the job to delete
	 */
	public void removeJob(String jobId) {
		jobDao.deleteJobById(jobId);
	}

	/**
	 * Adds a Job
	 * 
	 * @param job
	 *            The Job
	 */
	public void addJob(Job job) {
		JobEntity jobEntity = new JobEntity(job);
		jobDao.save(jobEntity);
	}

}