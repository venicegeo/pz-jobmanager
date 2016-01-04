#!/usr/bin/env bash

# Update repository to fetch latest OpenJDK
sudo add-apt-repository -y ppa:openjdk-r/ppa
sudo apt-get -y update

# Install required packages
sudo apt-get -y install openjdk-8-jdk maven

# Build the JobManager application
cd /vagrant/jobmanager
mvn clean package

# Updating hosts
echo "192.168.23.24  jobdb.dev" >> /etc/hosts
echo "192.168.33.12  kafka.dev" >> /etc/hosts

# Add an Upstart job to run our script upon machine boot
chmod 777 /vagrant/jobmanager/config/spring-start.sh
cp /vagrant/jobmanager/config/jobmanager.conf /etc/init/jobmanager.conf

# Run the JobManager application
cd /vagrant/jobmanager/config
./spring-start.sh