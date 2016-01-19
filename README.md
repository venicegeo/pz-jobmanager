# Job Manager

Internal component that provides persistence for Jobs as they pass throughout the application. This contains a [MongoDB](https://www.mongodb.org/) instance that stores all Jobs.

## Vagrant

Run ``vagrant up`` to start the Job Manager service and Database Instance. The Job Manager service and the Database are **not** intended to be accessed directly, and thus provide no convenient interface for access. Instead, internal Piazza components should communicate with the [Dispatcher](https://github.com/venicegeo/pz-dispatcher) component. The Dispatcher provides a REST Interface for synchronously querying Job Status, and consumes a series of Kafka topics that can be produced in order to modify the Job collections contained in the database. As a result, the [Dispatcher](https://github.com/venicegeo/pz-dispatcher) Vagrant machine should also be running in order to interact with the Job Manager's Vagrant machines. For Kafka messaging, the [Kafka Devbox](https://github.com/venicegeo/kafka-devbox) is also required to be running. 

## Running Locally

To run the Job Manager service locally (without Vagrant), perhaps through Eclipse or through CLI, navigate to the project directory and run ``mvn clean install -U spring-boot:run``. This will run a Tomcat server locally with the Gateway service running on port 8083. Even if running the Job Manager locally, you should still run the MongoDB instance inside of Vagrant by running ``vagrant up jobdb`` from the /config directory.

## Interface

The Dispatcher component interacts directly with the Job Manager. For updates, this is done through Kafka Messages such as ``Create-Job``. For querying the status of Jobs, this is done synchronously through a REST Endpoint exposed by the Job Manager and available at http://jobmanager.dev:8083/job/JOB_ID. *(Since this endpoint is not intended to be accessed directly, it will likely be eventually locked down so that only the Dispatcher is allowed access to it. However, for testing, it will remain open.)*

## Piazza Database & Jobs Collection

The MongoDB instance uses a database titled ``Piazza`` with a single ``Jobs`` collection. The interfaces exposed through the Dispatcher messaging will be simple CRUD-style functionality. The JSON stored in the Jobs collection will be stored using the [Common Job](https://github.com/venicegeo/pz-jobcommon)

```
{
	"type": "job"
	"jobId": "a10a04af-5e7e-4aea-b7de-f3dbc12e4279"
	"ready": false
	"status": "Submitted",
	"result": {},
	"progress": {
		"percentComplete": null
		"timeRemaining": null
		"timeSpent": null
	}
}
```
