#!/bin/sh
sudo /usr/lib/kafka/bin/kafka-server-stop.sh
sleep 5
sudo /usr/lib/kafka/bin/kafka-server-start.sh -daemon /etc/kafka/conf.dist/server.properties
