# -*- mode: ruby -*-

Vagrant.configure("2") do |config|
  config.vm.box = "xenial-current"
  config.vm.box_url = "https://cloud-images.ubuntu.com/xenial/current/xenial-server-cloudimg-amd64-vagrant.box"
  config.vm.synced_folder ".", "/vagrant", nfs: true
  config.vm.provider :virtualbox do |vb|
    vb.customize ["modifyvm", :id, "--memory", "1024"]
  end
  config.vm.define "dev", primary: true do |dev|
    # nginx
    config.vm.network :forwarded_port, guest: 5432, host: 15432
    config.vm.provision :shell,
      :keep_color => true,
      :path => "setup.sh"
   end
end
