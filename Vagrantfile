# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

	config.vm.box = "ubuntu/precise64"
	config.vm.hostname = "jobmanager.dev"
	config.vm.provision :shell, path: "jm-bootstrap.sh"
	config.vm.network :private_network, ip:"192.168.23.23"
	config.vm.network "forwarded_port", guest: 8080, host: 8081
	config.vm.synced_folder "./", "/vagrant/jobmanager"
	config.vm.provider "virtualbox" do |vb|
	  vb.customize ["modifyvm", :id, "--memory", "512"]
	end

end