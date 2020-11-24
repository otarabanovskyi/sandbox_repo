#at_least_once
#python-kafka sjould be installed
#If kafka and kafka-python modules are installed, both should be uninstalled. After it to install kafka-python module only.
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'gcp.orders.fct.btcusd.0',
    bootstrap_servers=['procamp-cluster-m:9092'],
    group_id='group-1',
    enable_auto_commit=False
)

def consume_messages():
    while True:
        message_batch = consumer.poll()

        for partition_batch in message_batch.values():
            for message in partition_batch:
                # do processing of message
                print(message.value.decode('utf-8'))

        # commits the latest offsets returned by poll
        consumer.commit()


if __name__ == '__main__':
    consume_messages()