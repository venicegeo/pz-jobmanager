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

import org.mongojack.DBCursor;
import org.mongojack.DBQuery;
import org.mongojack.DBSort;
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
	 * @param pageSize
	 *            the number of results per page
	 * @param order
	 *            "ascending" or "descending"
	 * @param status
	 *            The status of the Job
	 * @param userName
	 *            The username who submitted the Job
	 * @return The list of jobs
	 */
	public List<Job> getJobs(int page, int pageSize, String order) {
		// Get all of the Jobs.
		DBCursor<Job> cursor = getJobCollection().find();
		return getPaginatedResults(cursor, page, pageSize, order);
	}

	/**
	 * Gets a list of Jobs from the database for the user
	 * 
	 * @param page
	 *            the page number
	 * @param pageSize
	 *            the number of results per page
	 * @param order
	 *            "ascending" or "descending"
	 * @param userName
	 *            The username who submitted the Job
	 * @return The list of jobs
	 */
	public List<Job> getJobsForUser(int page, int pageSize, String order, String user) {
		DBCursor<Job> cursor = getJobCollection().find(DBQuery.is("submitterUserName", user));
		return getPaginatedResults(cursor, page, pageSize, user);
	}

	/**
	 * Gets a list of Jobs from the database containing the status
	 * 
	 * @param page
	 *            the page number
	 * @param pageSize
	 *            the number of results per page
	 * @param order
	 *            "ascending" or "descending"
	 * @param status
	 *            The status of the Job
	 * @return The list of jobs
	 */
	public List<Job> getJobsForStatus(int page, int pageSize, String order, String status) {
		DBCursor<Job> cursor = getJobCollection().find(DBQuery.is("status", status));
		return getPaginatedResults(cursor, page, pageSize, order);
	}

	/**
	 * Adds pagination to a DB Cursor set.
	 * 
	 * @param page
	 *            The page number to start
	 * @param pageSize
	 *            Results per page
	 * @param order
	 *            "ascending" or "descending"
	 * @return
	 */
	private List<Job> getPaginatedResults(DBCursor<Job> cursor, int page, int pageSize, String order) {
		// If sorting is enabled, then sort the response.
		if ((order != null) && (order.isEmpty() == false)) {
			if (order.equalsIgnoreCase("ascending")) {
				cursor = cursor.sort(DBSort.asc("submitted"));
			} else if (order.equalsIgnoreCase("descending")) {
				cursor = cursor.sort(DBSort.desc("submitted"));
			}
		}
		return cursor.skip(page * pageSize).limit(pageSize).toArray();
	}

}