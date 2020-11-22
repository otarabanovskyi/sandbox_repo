#!/bin/sh
#get btcusd topic offset (topic gcp.orders.fct.btcusd.0)
kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list procamp-cluster-m:9092,procamp-cluster-w-0:9092,procamp-cluster-w-1:9092 --topic gcp.orders.fct.btcusd.0
