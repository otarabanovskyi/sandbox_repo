# at least once
# python-kafka should be installed
# If kafka and kafka-python modules are installed,both should be uninstalled.
#   After it to install kafka-python module only.
# to install python modules on dataproc node:
#   - sudo /opt/conda/default/bin/python -m pip install kafka-python
from confluent_kafka import Consumer, KafkaError
import logging
import sys
import argparse

parser = argparse.ArgumentParser('Kafka "at least once" consumer')
parser.add_argument('-v', '--verbose', action='store_true', default=False,help='Enable debug output')
parser.add_argument('-r', '--remote_bootstrap', action='store_true', default=False,
                    help='remote kafka broker (to use with VPN), if not set, will be used localhost')
args = parser.parse_args()

logger = logging.getLogger('confluent')
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

settings = {
    'bootstrap.servers': lv_bootstrap_servers,
    'group.id': 'group-1',
    'client.id': 'client-1',
    'enable.auto.commit': False,
    'session.timeout.ms': 6000,
    # 'default.topic.config': {'auto.offset.reset': 'smallest'}
    'default.topic.config': {'auto.offset.reset': 'latest'}
}

c = Consumer(settings)

c.subscribe(['gcp.orders.fct.btcusd.0'])

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            print('Received message: {0}'.format(msg.value()))
            c.commit()
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass

finally:
    c.close()
