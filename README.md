#pz-jobmanager
The core purpose of the pz-jobmanager is for managing piazza jobs. For activities that are potentially time consuming such as the invocation of user services (e.g. algorithms), the orchestration of user services and loading of data results, Piazza leverages Apache Kafka for asynchronous messaging support. Requests are sent to the Gateway as Jobs. The Job Manager component obtains this information and creates job messages for the Workflow, Service Controller and Ingest components to obtain and work on. A unique jobId is used to track these jobs and is provided back to the NPE as a response to the job request. NPEs use the jobId to track the status of their job request. Leveraging Apache Kafka, the Workflow, Service Controller and Ingest components send updates about job status. Once the job is complete, data results are loaded onto S3 or PostGreSQL for NPEs to access.

***
## Requirements
Before building and running the pz-jobmanager project, please ensure that the following components are available and/or installed, as necessary:
- [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) (JDK for building/developing, otherwise JRE is fine)
- [Maven (v3 or later)](https://maven.apache.org/install.html)
- [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) (for checking out repository source)
- [Eclipse](https://www.eclipse.org/downloads/), or any maven-supported IDE
- [RabbitMQ](https://www.rabbitmq.com/download.html)
- [PostgreSQL](https://www.postgresql.org/download)
- [Apache Kafka](https://kafka.apache.org/quickstart)
- [Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/gsg/GetStartedWithS3.html) bucket access
- Access to Nexus is required to build

Ensure that the nexus url environment variable `ARTIFACT_STORAGE_URL` is set:

	$ export ARTIFACT_STORAGE_URL={Artifact Storage URL}

For additional details on prerequisites, please refer to the Piazza Developer's Guide [Core Overview](http://pz-docs.int.dev.east.paas.geointservices.io/devguide/02-pz-core/) or [Piazza Job Manager](http://pz-docs.int.dev.east.paas.geointservices.io/devguide/05-pz-jobmanager/) sections. Also refer to the [prerequisites for using Piazza](http://pz-docs.int.dev.east.paas.geointservices.io/devguide/03-jobs/) section for additional details.

***
## Setup, Configuring & Running

### Setup
Create the directory the repository must live in, and clone the git repository:

    $ mkdir -p {PROJECT_DIR}/src/github.com/venicegeo
	$ cd {PROJECT_DIR}/src/github.com/venicegeo
    $ git clone git@github.com:venicegeo/pz-jobmanager.git
    $ cd pz-jobmanager

>__Note:__ In the above commands, replace {PROJECT_DIR} with the local directory path for where the project source is to be installed.

### Configuring
As noted in the Requirements section, to build and run this project, RabbitMQ, PostgreSQL, and Kafka are required. The `application.properties` file controls URL information for these components it connects to.

### Running locally

To build and run the Job Manager service locally, pz-jobmanager can be run using Eclipse any maven-supported IDE. Alternatively, pz-jobmanager can be run through command line interface (CLI), by navigating to the project directory and run:

`mvn clean install -U spring-boot:run`

> __Note:__ This Maven build depends on having access to the `Piazza-Group` repository as defined in the `pom.xml` file. If your Maven configuration does not specify credentials to this Repository, this Maven build will fail.

This will run a Tomcat server locally with the Gateway service running on port ** 8083 **. Even if running the Job Manager locally, you should have a local PostgreSQL instance running.
