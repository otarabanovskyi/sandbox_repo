#!/bin/sh
#delete kafka topic gcp.orders.fct.btcusd.0
kafka-topics.sh --zookeeper localhost:2181 \
--topic gcp.orders.fct.btcusd.0 \
--delete
