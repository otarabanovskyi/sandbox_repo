#!/bin/sh
#purge kafka topic gcp.orders.fct.btcusd.0
/usr/lib/kafka/bin/kafka-configs.sh --zookeeper localhost:2181 --entity-type topics --alter --entity-name gcp.orders.fct.btcusd.0 --add-config retention.ms=1000
