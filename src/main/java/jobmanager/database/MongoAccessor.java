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

import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import model.job.Job;
import model.job.JobProgress;
import model.response.DataResourceListResponse;
import model.response.JobListResponse;
import model.response.Pagination;

import org.mongojack.DBCursor;
import org.mongojack.DBQuery;
import org.mongojack.DBQuery.Query;
import org.mongojack.DBSort;
import org.mongojack.DBUpdate;
import org.mongojack.JacksonDBCollection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoTimeoutException;

/**
 * Helper class to interact with and access the Mongo instance.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class MongoAccessor {
	@Value("${vcap.services.pz-mongodb.credentials.uri}")
	private String DATABASE_URI;
	@Value("${vcap.services.pz-mongodb.credentials.database}")
	private String DATABASE_NAME;
	@Value("${mongo.db.collection.name}")
	private String JOB_COLLECTION_NAME;
	private MongoClient mongoClient;

	public MongoAccessor() {
	}

	@PostConstruct
	private void initialize() {
		try {
			mongoClient = new MongoClient(new MongoClientURI(DATABASE_URI));
		} catch (Exception exception) {
			System.out.println("Error connecting to MongoDB Instance.");
			exception.printStackTrace();
		}
	}

	@PreDestroy
	private void close() {
		mongoClient.close();
	}

	/**
	 * Gets a reference to the MongoDB Client Object.
	 * 
	 * @return
	 */
	public MongoClient getClient() {
		return mongoClient;
	}

	/**
	 * Gets a reference to the MongoDB's Job Collection.
	 * 
	 * @return
	 */
	public JacksonDBCollection<Job, String> getJobCollection() {
		// MongoJack does not support the latest Mongo API yet. TODO: Check if
		// they plan to.
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(JOB_COLLECTION_NAME);
		return JacksonDBCollection.wrap(collection, Job.class, String.class);
	}

	/**
	 * Gets the total number of Jobs in the database
	 * 
	 * @return Number of jobs in the DB
	 */
	public long getJobsCount() {
		return getJobCollection().getCount();
	}

	/**
	 * Gets the number of Jobs for the specific status
	 * 
	 * @param status
	 *            The Status
	 * @return The number of Jobs for that status
	 */
	public int getJobStatusCount(String status) {
		return getJobCollection().find(DBQuery.is("status", status)).count();
	}

	/**
	 * Returns a Job that matches the specified ID.
	 * 
	 * @param jobId
	 *            Job ID
	 * @return The Job with the specified ID
	 */
	public Job getJobById(String jobId) throws ResourceAccessException {
		BasicDBObject query = new BasicDBObject("jobId", jobId);
		Job job;

		try {
			if ((job = getJobCollection().findOne(query)) == null) {
				return null;
			}
		} catch (MongoTimeoutException mte) {
			throw new ResourceAccessException("MongoDB instance not available.");
		}

		return job;
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
		// Construct the query based on the user parameters.
		Query query = DBQuery.empty();
		if ((userName != null) && (userName.isEmpty() == false)) {
			query.and(DBQuery.is("submitterUserName", userName));
		}
		if ((status != null) && (status.isEmpty() == false)) {
			query.and(DBQuery.is("status", status));
		}

		// Execute the query
		DBCursor<Job> cursor = getJobCollection().find(query);

		// Sort and order the Results
		if (order.equalsIgnoreCase("asc")) {
			cursor = cursor.sort(DBSort.asc(sortBy));
		} else if (order.equalsIgnoreCase("desc")) {
			cursor = cursor.sort(DBSort.desc(sortBy));
		}

		// Get the total count
		Integer size = new Integer(cursor.size());

		// Paginate the results
		List<Job> jobs = cursor.skip(page * perPage).limit(perPage).toArray();

		// Attach pagination information
		Pagination pagination = new Pagination(size, page, perPage, sortBy, order);

		// Create the Response and send back
		return new JobListResponse(jobs, pagination);
	}

	/**
	 * Updates the status of a Job.
	 * 
	 * @param jobId
	 *            The Job ID
	 * @param status
	 *            The Status
	 */
	public void updateJobStatus(String jobId, String status) {
		getJobCollection().update(DBQuery.is("jobId", jobId), DBUpdate.set("status", status));
	}

	/**
	 * Updates the Progress of a Job
	 * 
	 * @param jobId
	 *            The Job ID to update
	 * @param progress
	 *            The progres to set
	 */
	public void updateJobProgress(String jobId, JobProgress progress) {
		getJobCollection().update(DBQuery.is("jobId", jobId), DBUpdate.set("progress", progress));
	}

	/**
	 * Deletes a Job entry.
	 * 
	 * @param jobId
	 *            The ID of the job to delete
	 */
	public void removeJob(String jobId) {
		getJobCollection().remove(DBQuery.is("jobId", jobId));
	}

	/**
	 * Adds a Job
	 * 
	 * @param job
	 *            The Job
	 */
	public void addJob(Job job) {
		getJobCollection().insert(job);
	}

}