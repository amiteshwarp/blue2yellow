# -*- mode: ruby -*-
# vi: set ft=ruby :
# vagrant package --base local-server_default_1504103224899_32794 --output ent-development-package.box

# All Vagrant configuration is done below. The "2" in Vagrant.configure
# configures the configuration version (we support older styles for
# backwards compatibility). Please don't change it unless you know what
# you're doing.
Vagrant.configure("2") do |config|
  # The most common configuration options are documented and commented below.
  # For a complete reference, please see the online documentation at
  # https://docs.vagrantup.com.

  # Every Vagrant development environment requires a box. You can search for
  # boxes at https://vagrantcloud.com/search.
  config.vm.box = "ubuntu/bionic64"
  config.vm.box_version = "20190904.0.0"

  # Disable automatic box update checking. If you disable this, then
  # boxes will only be checked for updates when the user runs
  # `vagrant box outdated`. This is not recommended.
  # config.vm.box_check_update = false

  # Create a forwarded port mapping which allows access to a specific port
  # within the machine from a port on the host machine. In the example below,
  # accessing "localhost:8080" will access port 80 on the guest machine.
  # NOTE: This will enable public access to the opened port
  # config.vm.network "forwarded_port", guest: 80, host: 8080
  
  # Create a forwarded port mapping which allows access to a specific port
  # within the machine from a port on the host machine and only allow access
  # via 127.0.0.1 to disable public access
  #http://10.0.2.15:18080
  #http://127.0.0.1:8888
  config.vm.network :forwarded_port, guest: 8888, host: 8888
  config.vm.network :forwarded_port, guest: 4040, host: 4040
  config.vm.network :forwarded_port, guest: 18080, host: 18080
  config.vm.network :forwarded_port, guest: 22, host: 2222
  #config.vm.network "forwarded_port", guest: 15672, host: 15672, host_ip: "192.168.173.216"
  #config.vm.network "forwarded_port", guest: 80, host: 80, host_ip: "192.168.173.216"
  #config.vm.network "forwarded_port", guest: 26379, host: 26379, host_ip: "192.168.173.216"

  # Create a private network, which allows host-only access to the machine
  # using a specific IP.
  # config.vm.network "private_network", ip: "192.168.33.10"

  # Create a public network, which generally matched to bridged network.
  # Bridged networks make the machine appear as another physical device on
  # your network.
  # config.vm.network "public_network"

  # Provider-specific configuration so you can fine-tune various
  # backing providers for Vagrant. These expose provider-specific options.
  # Example for VirtualBox:
  # 
   config.vm.provider "virtualbox" do |vb|
     # Display the VirtualBox GUI when booting the machine
     # vb.gui = true
     # Customize the amount of memory on the VM:
     vb.memory = "4096"
   end
  #
  # View the documentation for the provider you are using for more
  # information on available options.
  
  # Enable provisioning with a shell script. Additional provisioners such as
  # Puppet, Chef, Ansible, Salt, and Docker are also available. Please see the
  # documentation for more information about their specific syntax and use.

  # Removed php stuff outside, if needed put inside.
  # aptitude install -y php-pear whois imagemagick curl ghostscript sysstat make libavcodec-extra libmp3lame0 openssl build-essential libssl-dev libgif7 nfs-common postgresql-client-common xvfb wkhtmltopdf clamav clamav-base clamav-daemon php7.0 php7.0-gd php7.0-imap php7.0-ldap php7.0-mbstring php7.0-mcrypt php7.0-mysql php7.0-pgsql php7.0-zip php7.0-xml php7.0-curl php7.0-bcmath php7.0-tidy php-redis php7.0-soap php7.0-pdo-dblib php7.0-pdo-mysql php7.0-pdo-odbc php7.0-pdo-pgsql php7.0-pdo-sqlite php7.0-odbc php7.0-intl php-intl tdsodbc php7.0-dev php7.0-pear
  # sudo update-alternatives --set php /usr/bin/php7.0 
  # sudo update-alternatives --set phpize /usr/bin/phpize7.0 
  # sudo update-alternatives --set php-config.1.gz /usr/bin/php-config7.0.1.gz 
  # sudo update-alternatives --set php-config /usr/bin/php-config7.0
  # pecl install libsodium-2.0.10
  config.vm.provision "shell", inline: <<-SHELL
      apt-get update
    	apt-get install oracle-java8-installer
    	apt-get install oracle-java8-set-default
      apt-get install python-software-properties
      add-apt-repository ppa:ondrej/php
      apt-get update
    	export JAVA_HOME=/usr/lib/jvm/java-8-oracle
    	export JRE_HOME=/usr/lib/jvm/java-8-oracle/jre
    	apt-get install -y aptitude
    	aptitude install -y python3 python3-pip scala
      ln -s /usr/bin/python3 /usr/bin/python
    	add-apt-repository ppa:webupd8team/java
    	apt-get update 
    	pip3 install pyspark jupyter py4j
    	wget http://mirrors.estointernet.in/apache/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz
    	tar -xf spark-2.4.4-bin-hadoop2.7.tgz
    	export PATH=$PATH:~/.local/bin
    	mv spark-2.4.4-bin-hadoop2.7 /usr/local/spark    	
  SHELL
  
  config.vm.provision :shell, :inline => "sudo jupyter notebook --no-browser --ip 0.0.0.0 --allow-root &", run: "always"
  config.vm.provision :shell, :inline => "mkdir -p /tmp/spark-events & sudo /usr/local/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer &", run: "always"
  config.vm.provision :shell, :inline => "sudo export PATH='/usr/local/scala/bin:$PATH' &", run: "always"
  config.vm.provision :shell, :inline => "sudo export PATH='/usr/local/spark/bin:$PATH' &", run: "always"
  config.vm.provision :shell, :inline => "sudo export SPARK_HOME='/usr/local/spark' &", run: "always"
  config.vm.provision :shell, :inline => "sudo export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH &", run: "always"
  config.vm.provision :shell, :inline => "sudo export PYSPARK_DRIVER_PYTHON='jupyter' &", run: "always"
  config.vm.provision :shell, :inline => "sudo export PYSPARK_DRIVER_PYTHON_OPTS='notebook' &", run: "always"
  config.vm.provision :shell, :inline => "sudo export PYSPARK_PYTHON=python3 &", run: "always"
  config.vm.provision :shell, :inline => "sudo export PATH=$SPARK_HOME:$PATH:~/.local/bin:$JAVA_HOME/bin:$JAVA_HOME/jre/bin &", run: "always"

  config.vm.synced_folder "c:/svn", "/srv/www/vhosts"
  # Share an additional folder to the guest VM. The first argument is
  # the path on the host to the actual folder. The second argument is
  # the path on the guest to mount the folder. And the optional third
  # argument is a set of non-required options.
  
  #Each folder need to physically present on windows system first
end