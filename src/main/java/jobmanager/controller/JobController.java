package jobmanager.controller;

import jobmanager.database.MongoAccessor;
import model.job.Job;
import model.response.ErrorResponse;
import model.response.JobStatusResponse;
import model.response.PiazzaResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.ResourceAccessException;

@RestController
public class JobController {
	@Autowired
	private MongoAccessor accessor;

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
	public PiazzaResponse getJobStatus(@PathVariable(value = "jobId") String jobId) {
		try {
			if (jobId.isEmpty()) {
				throw new Exception("No Job ID specified.");
			}
			// Query for the Job ID
			Job job = accessor.getJobById(jobId);
			// Return Job Status
			return new JobStatusResponse(job);
		} catch (ResourceAccessException exception) {
			return new ErrorResponse(jobId, "No Jobs matching the ID", "Job Manager");
		} catch (Exception exception) {
			exception.printStackTrace();
			return new ErrorResponse(jobId, "Error fetching Job: " + exception.getMessage(), "Job Manager");
		}
	}
}