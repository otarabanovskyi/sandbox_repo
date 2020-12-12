# at least once
# python-kafka should be installed
# If kafka and kafka-python modules are installed,both should be uninstalled.
#   After it to install kafka-python module only.
# to install python modules on dataproc node:
#   - sudo /opt/conda/default/bin/python -m pip install kafka-python
# module "pandas" should be reinstalled to update version
#   - sudo /opt/conda/default/bin/python -m pip uninstall pandas
#   - sudo /opt/conda/default/bin/python -m pip install pandas


from kafka import KafkaConsumer
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
    cnt = 0

    while True:
        logger.debug('******* poll start')
        message_batch = consumer.poll()
        cur_batch_df = pd.DataFrame()
        for partition_batch in message_batch.values():
            for message in partition_batch:
                # do processing of message
                logger.debug('Received message: ' + message.value.decode('utf-8'))
                try:
                    msg_df = pd.json_normalize(json.loads(message.value))
                    cnt = cnt + 1
                    cur_batch_df = pd.concat([cur_batch_df, msg_df])
                except UnicodeDecodeError:
                    logger.debug('Message unicode decode error: ' + str(message.value))
                except Exception as e:
                    logger.error("******* Error: ", sys.exc_info()[0])
        logger.debug('******* Current batch\n' + str(cur_batch_df))
        full_df = pd.concat([full_df, cur_batch_df])
        full_df = full_df.sort_values(['data.price'], ascending=[False])
        full_df = full_df.head(10)
        print('\n****************** ' + str(cnt) + ' messages are processed. Top 10 bitcoin transactions based on '
                                                   'price field (descending):')
        print(full_df[['event', 'data.id_str', 'data.amount_str', 'data.price_str']], '\n')
        # commits the latest offsets returned by poll
        consumer.commit()


if __name__ == '__main__':
    consume_messages()
