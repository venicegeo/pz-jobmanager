Running pz-jobmanager locally:

To run the Job Manager service locally, perhaps through Eclipse or through CLI, navigate to the project directory and run:

`mvn clean install -U spring-boot:run`

To build and run this project, software such as RabbitMQ and PostgreSQL are required.  For details on these prerequisites, refer to the
[Piazza Developer's Guide](https://pz-docs.geointservices.io/devguide/index.html#_piazza_core_overview).

NOTE: This Maven build depends on having access to the `Piazza-Group` repository as defined in the `pom.xml` file. If your Maven configuration does not specify credentials to this Repository, this Maven build will fail. 

This will run a Tomcat server locally with the Gateway service running on port 8083. Even if running the Job Manager locally, you should have a local PostgreSQL instance running.

