package main.java.jobmanager;

import main.java.jobmanager.messaging.JobMessager;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;

@SpringBootApplication
public class Application extends SpringBootServletInitializer {

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
		return builder.sources(Application.class);
	}

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);

		// Spin up the Kafka Consumer that will listen for messages.
		JobMessager jobMessager = new JobMessager();
		jobMessager.initialize();

	}
}