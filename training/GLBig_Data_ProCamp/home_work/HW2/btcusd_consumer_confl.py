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
import pandas as pd
import json

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
    logger.info('******* Debug output is enabled')
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

full_df = pd.DataFrame({'data.id': pd.Series([], dtype='int'),
                        'data.id_str': pd.Series([], dtype='str'),
                        'data.order_type': pd.Series([], dtype='int'),
                        'data.datetime': pd.Series([], dtype='str'),
                        'data.microtimestamp': pd.Series([], dtype='str'),
                        'data.amount': pd.Series([], dtype='float'),
                        'data.amount_str': pd.Series([], dtype='str'),
                        'data.price': pd.Series([], dtype='float'),
                        'data.price_str': pd.Series([], dtype='str'),
                        'channel': pd.Series([], dtype='str'),
                        'event': pd.Series([], dtype='str')})

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            logger.debug('Received message: {0}'.format(msg.value()))
            mess_df = pd.json_normalize(json.loads(msg.value()))
            full_df = pd.concat([full_df, mess_df])
            full_df = full_df.sort_values(['data.price'], ascending=[False])
            full_df = full_df.head(10)
            print('\n****************** top 10 bitcoin transactions based on price field (descending):')
            #print(full_df[['event', 'channel', 'data.id', 'data.price']])
            print(full_df[['event', 'data.id_str', 'data.amount_str', 'data.price_str']], '\n')
            c.commit()
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            logger.debug('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
        else:
            logger.error('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass

finally:
    c.close()

