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
package jobmanager.messaging;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import jobmanager.messaging.handler.RequestJobHandler;
import jobmanager.messaging.handler.UpdateStatusHandler;
import model.logger.Severity;
import model.request.PiazzaJobRequest;
import model.status.StatusUpdate;
import util.PiazzaLogger;

/**
 * Interacts with the Jobs Collection in the database based on Kafka messages received.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class JobMessager {
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private UpdateStatusHandler updateStatusHandler;
	@Autowired
	private RequestJobHandler requestJobHandler;
	@Autowired
	private ObjectMapper mapper;

	@Value("${SPACE}")
	private String SPACE;

	private static final Logger LOG = LoggerFactory.getLogger(JobMessager.class);

	public JobMessager() {
		// Expected for Component instantiation
	}

	/**
	 * Processes a message coming in through the queue to update a job
	 * 
	 * @param message
	 *            The Job Message Update, tied to the StatusUpdate POJO
	 */
	@RabbitListener(queues = "UpdateJob-${SPACE}")
	public void processUpdateMessage(String statusUpdateString) {
		try {
			// Get the POJO
			StatusUpdate statusUpdate = mapper.readValue(statusUpdateString, StatusUpdate.class);
			// Process the Message
			updateStatusHandler.process(statusUpdate);
		} catch (IOException exception) {
			String error = String.format("Error Reading Status Message from Queue %s", exception.getMessage());
			LOG.error(error, exception);
			logger.log(error, Severity.ERROR);
		}
	}

	/**
	 * Processes a message coming in throughu the queue to request a job
	 * 
	 * @param requestJob
	 *            The Job Request, tied to the PiazzaJobRequest POJO
	 */
	@RabbitListener(queues = "RequestJob-${SPACE}")
	public void processRequestMessage(String requestJobString) {
		try {
			// Get the POJO
			PiazzaJobRequest jobRequest = mapper.readValue(requestJobString, PiazzaJobRequest.class);
			// Process the Message
			requestJobHandler.process(jobRequest, jobRequest.jobId);
		} catch (IOException exception) {
			String error = String.format("Error Reading Job Request Message from Queue %s", exception.getMessage());
			LOG.error(error, exception);
			logger.log(error, Severity.ERROR);
		}
	}

}
