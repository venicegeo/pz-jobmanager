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
package jobmanager.messaging.handler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import jobmanager.database.MongoAccessor;
import model.job.Job;
import model.job.type.AbortJob;
import model.request.PiazzaJobRequest;
import model.status.StatusUpdate;
import util.PiazzaLogger;

/**
 * Handles the request for Aborting a Job by updating the Job table with the new Status.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class AbortJobHandler {
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private MongoAccessor accessor;

	/**
	 * Processes a Job request to cancel a Piazza job.
	 * 
	 * @param request
	 *            Job request.
	 */
	public void process(PiazzaJobRequest request) throws Exception {
		AbortJob abortJob = (AbortJob) request.jobType;
		Job jobToCancel = accessor.getJobById(abortJob.getJobId());
		if (jobToCancel == null) {
			throw new Exception(String.format("No job could be founding matching Id %s", abortJob.getJobId()));
		}
		// Ensure the user has permission to cancel the Job.
		if ((jobToCancel.createdBy == null) || (!jobToCancel.createdBy.equals(request.createdBy))) {
			throw new Exception(
					String.format("Could not Abort Job %s because it was not requested by the originating user.", abortJob.getJobId()));
		}
		String currentStatus = jobToCancel.status;
		if ((currentStatus.equals(StatusUpdate.STATUS_RUNNING)) || (currentStatus.equals(StatusUpdate.STATUS_PENDING))
				|| (currentStatus.equals(StatusUpdate.STATUS_SUBMITTED))) {
			accessor.updateJobStatus(abortJob.getJobId(), StatusUpdate.STATUS_CANCELLING);
		} else {
			String error = String.format("Could not Abort Job %s because it is no longer running.", abortJob.getJobId());
			logger.log(error, PiazzaLogger.INFO);
		}
	}
}
