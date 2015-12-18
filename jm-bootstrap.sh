#!/usr/bin/env bash

# Update repository to fetch latest OpenJDK
sudo add-apt-repository -y ppa:openjdk-r/ppa
sudo apt-get -y update

# Install required packages
sudo apt-get -y install openjdk-8-jdk maven tomcat7

# Ensure Tomcat starts with each bootup
sudo update-rc.d tomcat7 defaults

# Build the JobManager application
cd /vagrant/jobmanager
mvn clean package

# Copy the Gateway WAR file to the Tomcat /webapps directory
cp target/piazza-jobmanager*.war /var/lib/tomcat7/webapps/jobmanager.war

# Start the Tomcat server
sudo service tomcat7 start
