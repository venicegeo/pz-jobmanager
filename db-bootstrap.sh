#!/usr/bin/env bash

# Install software requirements
# See: https://docs.mongodb.org/v3.0/tutorial/install-mongodb-on-ubuntu/
sudo apt-get -y install mongodb
sudo service mongodb start

# Startup the database upon boot
sudo update-rc.d mongodb defaults