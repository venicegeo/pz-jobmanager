package main.java.jobmanager;

import java.net.UnknownHostException;

import main.java.jobmanager.messaging.JobMessager;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;

import com.mongodb.MongoClient;

@SpringBootApplication
public class Application extends SpringBootServletInitializer {
	private static final String DATABASE_HOST = "jobdb.dev";
	private static final int DATABASE_PORT = 27017;

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
		return builder.sources(Application.class);
	}

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);

		// Instantiate the Database connection
		try {
			MongoClient mongoClient = new MongoClient(DATABASE_HOST, DATABASE_PORT);
			// Spin up the Kafka Consumer that will listen for messages.
			JobMessager jobMessager = new JobMessager(mongoClient);
			jobMessager.initialize();
		} catch (UnknownHostException exception) {
			System.out.println("Error connecting to Mongo DB: " + exception.getMessage());
		}

	}
}