$setup_oonipipeline = <<SCRIPT
apt-get update
apt-get install -y python-dev python-pip postgresql-9.4 postgresql-client-9.4 libpq-dev

sudo -u postgres psql -c "CREATE USER ooni WITH PASSWORD 'changeme'"
sudo -u postgres psql -c "CREATE DATABASE ooni_pipeline"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE ooni_pipeline to ooni"

mkdir -p /data/ooni/private/reports-raw/normalised/
mkdir -p /data/ooni/private/bridge_reachability/
echo "{}" > /data/ooni/private/bridge_reachability/bridge_db.json
mkdir -p /data/ooni/public
chmod -R 777 /data/ooni/

cd /data/ooni-pipeline/
pip install -r requirements.txt
echo "Now:"
echo "1. vagrant ssh"
echo "2. cd /data/ooni-pipeline/"
echo "3. Edit client.cfg (maybe copy from client.cfg.example)"
echo "4. PYTHONPATH=`pwd` luigi --module pipeline.batch.daily_workflow ListReportsAndRun --date-interval 2016 --local-scheduler"
SCRIPT

Vagrant.configure("2") do |config|
  # Use Debian jessie with the vboxfs contrib add-on
  config.vm.box = "debian/contrib-jessie64"

  config.vm.synced_folder ".", "/data/ooni-pipeline"

  if File.directory?("../ooni-backend")
    config.vm.synced_folder "../ooni-backend", "/data/ooni-backend"
  end

  config.vm.provision :shell, :inline => $setup_oonipipeline

end
