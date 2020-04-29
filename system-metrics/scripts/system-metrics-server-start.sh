#!/bin/bash
#
# Fault HTTP Server installation and setup script
#

#function to install and enable collectd on a host
function install_collectd() {
  set -e

  if [ $EUID -ne 0 ]; then
      sudo "$0" "$@"
      exit $?
  fi

  source /etc/os-release

  function print_usage {
      >&2 echo 'usage: install-collectd "INSTALL_DIR" "OWNER"'
  }

  INSTALL_DIR=$1
  OWNER=$2

  if [ ! $INSTALL_DIR ]; then
      print_usage
      echo "error: missing first argument INSTALL_DIR"
      exit 1
  fi

  if [ ! $OWNER ]; then
      print_usage
      echo "error: missing second argument OWNER"
      exit 1
  fi

  COLLECTD_PACKAGES="collectd"
  COLLECTD_USER=$OWNER
  COLLECTD_GROUP="collectd"
  COLLECTD_CONF_DIR="/etc/collectd"

  # Users belonging to the collectd group have read/write access to the
  # collectd IPC socket.
  groupadd --force --system ${COLLECTD_GROUP}

  # Install the minimal set of collectd services
  apt-get update -y
  apt-get --no-install-recommends install -y ${COLLECTD_PACKAGES}
  apt-get install dos2unix

  #create install directory if not present
  mkdir -p ${INSTALL_DIR}/Resources

  #copy collect configuration to Resources folder
  echo """
  # collectd configuration file.  For more information on how to configure collectd
  # plugins, see https://collectd.org/documentation/manpages/collectd.conf.5.shtml#plugin_unixsock,
  # or refer to the manpage collectd.conf.

  BaseDir \"/var/lib/collectd\"
  PIDFile \"/run/collectd/collectd.pid\"
  Interval 1.0

  LoadPlugin cpu
  LoadPlugin memory
  LoadPlugin disk
  LoadPlugin interface

  <Plugin cpu>
      ReportByState true
      ReportByCpu true
      ValuesPercentage true
  </Plugin>

  <Plugin disk>
      Disk \"sdb1\"
      Disk \"sda1\"
  </Plugin>

  <Plugin interface>
    Interface \"lo\"
    Interface \"sit0\"
    IgnoreSelected true
  </Plugin>

  LoadPlugin csv
  <Plugin csv>
    DataDir \"/var/lib/collectd/csv\"
    StoreRates true
  </Plugin>""" > ${INSTALL_DIR}/Resources/collectd.conf

  # Replace the collectd configuration
  find ${INSTALL_DIR} -name "*.conf" | xargs dos2unix
  install --verbose --owner="${COLLECTD_USER}" --group="${COLLECTD_GROUP}" --mode="644" "${INSTALL_DIR}/Resources/collectd.conf" "${COLLECTD_CONF_DIR}/collectd.conf"

  # Register collectd unit file
  systemctl stop collectd
  systemctl daemon-reload
  systemctl enable collectd
}

while getopts j:p:q: option
do
  case "${option}"
  in
  j) HTTP_SERVER_JAR_PATH=${OPTARG};;
  p) HTTP_PORT=${OPTARG};;
  q) HTTP_ADMIN_PORT=${OPTARG};;
  esac
done

if [ -z "$HTTP_SERVER_JAR_PATH" ]
then
  echo "system metrics http server path not specified, Please Specify it"
  exit 1;
fi

if [ -z "$HTTP_PORT" ]
then
  echo "system metrics http port not specified, Please Specify it"
  exit 1;
fi

if [ -z "$HTTP_ADMIN_PORT" ]
then
  echo "system metrics http admin port not specified, Please Specify it"
  exit 1;
fi

echo "system metrics http server jar path is : $HTTP_SERVER_JAR_PATH"

if ! (hdfs dfs -test -e $HTTP_SERVER_JAR_PATH)
then
  echo "given jar file does not exist, Please give correct jar path"
  exit 1
fi

#call install collectd function
install_collectd "/opt/collectd/" "root"

SYSTEM_METRICS_SERVER_FOLDER="/opt/system-metrics-server/"
echo "creating a directory in ${SYSTEM_METRICS_SERVER_FOLDER} to store the jar and config file"
mkdir -p ${SYSTEM_METRICS_SERVER_FOLDER}
echo "created the directory"
echo "copying jar path from remote storage to ${SYSTEM_METRICS_SERVER_FOLDER}"
hdfs dfs -copyToLocal -f $HTTP_SERVER_JAR_PATH ${SYSTEM_METRICS_SERVER_FOLDER}
echo "copied the file"

###create a config.yml file for the dropwizard application
echo """
version: 0.0.1

# config file for dropwizard application
server:
  applicationConnectors:
  - type: http
    port: ${HTTP_PORT}
  adminConnectors:
  - type: http
    port: ${HTTP_ADMIN_PORT}
  requestLog:
    appenders:
      - type: file
        currentLogFilename: /var/log/system-metrics-server-access.log
        threshold: ALL
        archive: true
        archivedLogFilenamePattern: /var/log/system-metrics-server-access-%d.log.gz
        archivedFileCount: 3
logging:
  level: INFO
  appenders:
    - type: file
      currentLogFilename: /var/log/system-metrics-server-application.log
      threshold: ALL
      archive: true
      archivedLogFilenamePattern: /var/log/system-metrics-server-application-%d.log
      archivedFileCount: 3
""" > ${SYSTEM_METRICS_SERVER_FOLDER}/config.yml

#create a systemd service file for system-metrics-server
echo "creating a systemd service file"
echo """[Unit]
Description=HTTP Service For System Metrics
After=sysinit.target
Before=shutdown.target

[Service]
User=root
Type=simple
RemainAfterExit=no
WorkingDirectory=${SYSTEM_METRICS_SERVER_FOLDER}
ExecStart=/bin/bash -c '/usr/bin/java -cp system-metrics-1.0-SNAPSHOT.jar:\$(hadoop classpath) ms.CollectdRestApp server config.yml'
Type=simple
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target""" > /etc/systemd/system/system-metrics-server.service
dos2unix /etc/systemd/system/system-metrics-server.service
echo "systemd service file created"

###stop system-metrics-server if already running
if (sudo systemctl is-active --quiet system-metrics-server)
then
  echo "system-metrics-server http service is already active, will stop it first"
  if (sudo systemctl stop system-metrics-server)
  then
    echo "stopped the running fault injection service"
  else
    echo "Unable to stop the service"
  fi
fi

###start the system metrics service
sudo systemctl daemon-reload
sudo systemctl enable system-metrics-server.service
sudo systemctl start system-metrics-server
if (sudo systemctl is-active --quiet system-metrics-server)
then
  echo "system-metrics-server service has started and is active"
else
  echo "system-metrics-server service did not start"
fi
