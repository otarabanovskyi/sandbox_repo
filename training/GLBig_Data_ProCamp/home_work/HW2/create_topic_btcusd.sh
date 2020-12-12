#!/bin/sh
#The script should be executed on dataproc cluster (master node). Add execution permission before a call: sudo chmod u+x *.sh
#Create Kafka topic for bitcoin-usd transactions
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 1 --topic gcp.orders.fct.btcusd.0
#Check topic
echo "check topic gcp.orders.fct.btcusd.0"
kafka-topics.sh --list --zookeeper localhost:2181 | grep gcp.orders.fct.btcusd.0
#Add user "nifi" to group "kafka"
#optional (for kafka concumer on local PC): sudo usermod -a -G kafka mo_tarabanovskyi_gmail_com
sudo usermod -a -G kafka nifi
sudo usermod -a -G kafka mo_tarabanovskyi_gmail_com
#Check user "nifi" groups
echo "check nifi groups"
groups nifi | grep kafka
