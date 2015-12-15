package main.java.jobmanager.controller;

import model.response.JobStatusResponse;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class JobController {

	/**
	 * Returns the Job Status and potential Results of the specified Job ID.
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
	public JobStatusResponse getJobStatus(@PathVariable(value = "jobId") String jobId) {
		return new JobStatusResponse("TestJobID");
	}
}