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
package jobmanager.test;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import jobmanager.database.MongoAccessor;
import model.job.result.type.TextResult;
import model.status.StatusUpdate;

/**
 * Tests (what we can) for the MongoDB Accessor instance
 * 
 * @author Patrick.Doody
 *
 */
public class MongoTests {
	@InjectMocks
	private MongoAccessor accessor;

	/**
	 * Setup tests
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
	}

	/**
	 * Tests updating a status with a result. Fails b/c no Mongo connection.
	 */
	@Test(expected = Exception.class)
	public void testUpdateStatusWithResult() throws Exception {
		// Mock
		StatusUpdate mockUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);
		mockUpdate.setResult(new TextResult("Test"));

		// Test
		accessor.updateJobStatus("123456", mockUpdate);
	}

	/**
	 * Tests updating a status without a result. Fails b/c no Mongo connection.
	 */
	@Test(expected = Exception.class)
	public void testUpdateStatusWithoutResult() throws Exception {
		// Mock
		StatusUpdate mockUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);

		// Test
		accessor.updateJobStatus("123456", mockUpdate);
	}

	/**
	 * Tests accessor
	 */
	@Test
	public void testClient() {
		accessor.getClient();
	}
}
