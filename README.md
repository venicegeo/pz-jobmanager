Running pz-jobmanager locally:

To run the Job Manager service locally, perhaps through Eclipse or through CLI, navigate to the project directory and run:

mvn clean install -U spring-boot:run

This will run a Tomcat server locally with the Gateway service running on port 8083. Even if running the Job Manager locally, you should still run the MongoDB instance inside of Vagrant by running

vagrant up jobdb

from the /config directory.
