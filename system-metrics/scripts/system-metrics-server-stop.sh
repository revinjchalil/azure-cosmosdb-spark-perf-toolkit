#!/bin/bash
if (sudo systemctl is-active --quiet system-metrics-server)
then
  echo "system-metrics-server http service is already active, will stop it first"
  if (sudo systemctl stop system-metrics-server)
  then
    echo "stopped the running fault injection service"
  else
    echo "Unable to stop the service"
  fi
else
  echo "No service running"
fi