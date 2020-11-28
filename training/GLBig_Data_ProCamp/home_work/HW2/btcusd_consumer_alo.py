# at least once
# python-kafka should be installed
# If kafka and kafka-python modules are installed,both should be uninstalled.
#   After it to install kafka-python module only.
# to install python modules on dataproc node:
#   - sudo /opt/conda/default/bin/python -m pip install kafka-python
#   - sudo /opt/conda/default/bin/python -m pip install logging

from kafka import KafkaConsumer
import logging
import sys
import argparse

parser = argparse.ArgumentParser('Kafka "at least once" consumer')
parser.add_argument('-v', '--verbose', action='store_true', default=False,help='Enable debug output')
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

consumer = KafkaConsumer(
    'gcp.orders.fct.btcusd.0',
    bootstrap_servers=['localhost:9092'],
    group_id='group-1',
    enable_auto_commit=False
)
logger.info('******* consumer init is done')


def consume_messages():
    while True:
        logger.debug('******* poll start')
        message_batch = consumer.poll()
        for partition_batch in message_batch.values():
            for message in partition_batch:
                # do processing of message
                print(message.value.decode('utf-8'))
        logger.debug('******* poll completed')

        # commits the latest offsets returned by poll
        consumer.commit()


if __name__ == '__main__':
    consume_messages()
