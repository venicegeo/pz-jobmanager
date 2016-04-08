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
package jobmanager.messaging.event;

import java.util.HashMap;
import java.util.Map;
import model.job.Job;

/**
 * Represents an Job Event that is fired to the pz-workflow component upon
 * successful job status change event request.
 * 
 * This populates the JSON Payload for the event that is fired to the
 * pz-workflow.
 * 
 * The template EventType for this Event has to be registered with the
 * pz-workflow before events of this type can be processed by that component.
 * 
 * @author Sonny.Saniev
 * 
 */
public class JobEvent {
	public String type;
	public String date;
	public Map<String, Object> data = new HashMap<String, Object>();

	/**
	 * Creates a job event based on the Data Resource that was created by
	 * the job manager component. This will parse the necessary properties from the
	 * Job and DataResource objects and insert them into the proper format
	 * defined by the job manager EventType.
	 * 
	 * @param id
	 *            The ID of the Job (type) as registered initially with
	 *            pz-workflow
	 * @param job
	 *            The Job that was processed and resulted in the creation of the
	 *            Data Resource
	 * @param jobStatus
	 *            The status of the job.
	 */
	public JobEvent(String id, Job job, String jobStatus) {
		// The Type/ID as registered initially with the service.
		type = id;
		// Set the date
		date = job.getSubmittedString();
		
		// Populate the Map fields
		data.put("status", jobStatus);
	}
}
