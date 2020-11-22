#!/bin/sh
#The script should be executed on dataproc cluster (master node). Add execution permission before a call: chmod u+x create_topic_btcusd.sh
#Create Kafka topic for bitcoin-usd transactions
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 1 --topic gcp.orders.fct.btcusd.0
#Check topic
echo "check topic gcp.orders.fct.btcusd.0"
kafka-topics.sh --list --zookeeper localhost:2181 | grep gcp.orders.fct.btcusd.0
#Add user "nifi" to group "kafka"
sudo usermod -a -G kafka nifi
#Check user "nifi" groups
echo "check nifi groups"
groups nifi | grep kafka
