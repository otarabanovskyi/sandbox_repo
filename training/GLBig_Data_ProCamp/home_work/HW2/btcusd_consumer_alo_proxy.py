#at_least_once
#python-kafka sjould be installed
#If kafka and kafka-python modules are installed, both should be uninstalled. After it to install kafka-python module only.
from kafka import KafkaConsumer
#PySocks module should be installed
import socks
import socket
#import os
#os.environ['NO_PROXY'] = 'socks5://localhost:1080'

def bind_proxy_port(enable=True):
    try:
        if enable == True:
            socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, 'localhost', 1080)
            socket.socket = socks.socksocket
            print('Proxy is enabled')
        return True
    except:
        raise Exception("unable to set proxy 127.0.0.1:1080")

bind_proxy_port()
consumer = KafkaConsumer(
    'gcp.orders.fct.btcusd.0',
    bootstrap_servers=['procamp-cluster-m:9092'],
    group_id='group-1',
    enable_auto_commit=False
)
print('consumer init is done')

def consume_messages():
    while True:
        print('start poll')
        message_batch = consumer.poll()
        print('poll')

        for partition_batch in message_batch.values():
            for message in partition_batch:
                # do processing of message
                print(message.value.decode('utf-8'))

        # commits the latest offsets returned by poll
        consumer.commit()

if __name__ == '__main__':
    consume_messages()