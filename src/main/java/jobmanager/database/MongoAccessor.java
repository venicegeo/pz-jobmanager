package main.java.jobmanager.database;

import java.net.UnknownHostException;

import model.job.Job;

import org.mongojack.JacksonDBCollection;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/**
 * Helper class to interact with and access the Mongo instance.
 * 
 * @author Patrick.Doody
 * 
 */
public class MongoAccessor {
	/**
	 * Singleton instance.
	 */
	private static final MongoAccessor instance = new MongoAccessor();

	private static final String DATABASE_HOST = "localhost";
	private static final int DATABASE_PORT = 27017;
	private static final String DATABASE_NAME = "Piazza";
	private static final String JOB_COLLECTION_NAME = "Jobs";
	private MongoClient mongoClient;

	protected MongoAccessor() {
		try {
			mongoClient = new MongoClient(DATABASE_HOST, DATABASE_PORT);
		} catch (UnknownHostException exception) {
			System.out.println("Error connecting to MongoDB Instance.");
			exception.printStackTrace();
		}
	}

	/**
	 * Thread-safe Singleton Accessor
	 * 
	 * @return
	 */
	public static MongoAccessor getInstance() {
		return instance;
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
}