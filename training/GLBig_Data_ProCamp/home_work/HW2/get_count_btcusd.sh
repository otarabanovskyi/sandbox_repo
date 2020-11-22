#!/bin/sh
#gmet message count (topic gcp.orders.fct.btcusd.0)
kafka-console-consumer.sh  --from-beginning \
--bootstrap-server localhost:9092 --property print.key=true  \
--property print.value=false --property print.partition \
--topic gcp.orders.fct.btcusd.0 --timeout-ms 5000 | tail -n 10|grep "Processed a total of"
