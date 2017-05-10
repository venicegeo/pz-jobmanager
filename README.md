Running pz-jobmanager locally:

To run the Job Manager service locally, perhaps through Eclipse or through CLI, navigate to the project directory and run:

`mvn clean install -U spring-boot:run`

NOTE: This Maven build depends on having access to the `Piazza-Group` repository as defined in the `pom.xml` file. If your Maven configuration does not specify credentials to this Repository, this Maven build will fail. 

This will run a Tomcat server locally with the Gateway service running on port 8083. Even if running the Job Manager locally, you should still run the MongoDB instance inside of Vagrant by running

`vagrant up jobdb`

from the `/config` directory.
