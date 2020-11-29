# at least once
# python-kafka should be installed
# If kafka and kafka-python modules are installed,both should be uninstalled.
#   After it to install kafka-python module only.
# to install python modules on dataproc node:
#   - sudo /opt/conda/default/bin/python -m pip install kafka-python

from kafka import KafkaConsumer
import logging
import sys
import argparse
import json
import pandas as pd

parser = argparse.ArgumentParser('Kafka "at least once" consumer')
parser.add_argument('-v', '--verbose', action='store_true', default=False,help='Enable debug output')
parser.add_argument('-r', '--remote_bootstrap', action='store_true', default=False,
                    help='remote kafka broker (to use with VPN), if not set, will be used localhost')
args = parser.parse_args()

logger = logging.getLogger('kafka')
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

if args.verbose:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

if args.remote_bootstrap:
    logger.info('******* Set remote kafka broker')
    # Should be set master node internal hostname in format <host name>.<region>.<project id>.internal
    # open vpn server should use configuration:
    #   sandbox_repo\training\GLBig_Data_ProCamp\infra\vpn\openvpn-configuration.sh
    lv_bootstrap_servers = 'procamp-cluster-m.us-east1-b.c.bigdata-procamp-1add8fad.internal'
else:
    logger.info('******* Set local kafka broker')
    lv_bootstrap_servers = 'localhost:9092'

consumer = KafkaConsumer(
    'gcp.orders.fct.btcusd.0',
    bootstrap_servers=[lv_bootstrap_servers],
    group_id='group-1',
    enable_auto_commit=False
)
logger.info('******* consumer init is done')


def consume_messages():
    while True:
        logger.debug('******* poll start')
        message_batch = consumer.poll()
        '''for partition_batch in message_batch.values():
            for message in partition_batch:
                # do processing of message
                print(message.value.decode('utf-8'))'''
        print(message_batch.values())
        print('************ message_batch:' + str(len(partition_batch)))
        # logger.debug('******* poll completed: ' || message_batch.values())

        # commits the latest offsets returned by poll
        consumer.commit()


if __name__ == '__main__':
    consume_messages()
