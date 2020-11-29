#!/bin/sh
sudo /opt/nifi/nifi-current/bin/nifi.sh stop
sleep 5
sudo /opt/nifi/nifi-current/bin/nifi.sh start
