#at_least_once
#python-kafka sjould be installed
#If kafka and kafka-python modules are installed, both should be uninstalled. After it to install kafka-python module only.
#to install on dataproc node: sudo /opt/conda/default/bin/python -m pip install kafka-python
from kafka import KafkaConsumer
import logging
import sys
logger = logging.getLogger('kafka')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.DEBUG)
#logger.setLevel(logging.INFO)

consumer = KafkaConsumer(
    'gcp.orders.fct.btcusd.0',
    bootstrap_servers=['localhost:9092'],
    group_id='group-1',
    enable_auto_commit=False
)
print('consumer init is done')

def consume_messages():
    while True:
        print('poll start')
        message_batch = consumer.poll()
        print('poll completed')
        for partition_batch in message_batch.values():
            for message in partition_batch:
                # do processing of message
                print(message.value.decode('utf-8'))

        # commits the latest offsets returned by poll
        consumer.commit()

if __name__ == '__main__':
    consume_messages()