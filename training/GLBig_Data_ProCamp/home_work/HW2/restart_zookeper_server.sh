#!/bin/sh
sudo /usr/lib/kafka/bin/zookeeper-server-stop.sh
sleep 5
sudo /usr/lib/kafka/bin/zookeeper-server-start.sh -daemon /etc/zookeeper/conf.dist/zoo.cfg
