import os
import sys
import logging
import json
from random import uniform, choice

import paho.mqtt.client as mqtt
from Sparkplug.client_libraries.python import sparkplug_b
from Sparkplug.client_libraries.python.sparkplug_b import *

AMOUNT_OF_PUBLISHED_TRIES = 5

DDATA_FILE_NAME = 'ddata'

sys.path.insert(0, "../client_libraries/python/")

brocker_host = os.environ['BROCKER_HOST']
topic = os.environ['TOPIC']

logger = logging.getLogger()
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

# Matching json field to sparkplug MetricDataType
data_type_matching = {
    'Int8': MetricDataType.Int8,
    'Int16': MetricDataType.Int16,
    'Int32': MetricDataType.Int32,
    'Int64': MetricDataType.Int64,
    'Float': MetricDataType.Float,
    'Double': MetricDataType.Double,
    'Boolean': MetricDataType.Boolean,
    'String': MetricDataType.String,
}


def _generate_double():
    return round(uniform(0.2, 4), 2)


# The callback for when the client receives a CONNACK response from the server.
def on_connect(mqttc, obj, flags, rc):
    if rc == 0:
        logger.info('Connected to the broker. Code: %s', rc)
    else:
        logger.info('Failed to connected to the broker. Code: %s', rc)


def on_publish(mqttc, obj, mid):
    print("mid: " + str(mid))


def on_message(mqttc, obj, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))


def publish_dbirth(dbirth):
    print("Publishing Device Birth")

    # Get the payload
    payload = sparkplug_b.getDeviceBirthPayload()

    # Add some device metrics with only name and alias
    __add_dbirth_metric_from_json(payload, dbirth)

    # Publish the initial data with the Device BIRTH certificate
    total_byte_array = bytearray(payload.SerializeToString())
    client.publish(topic, total_byte_array, 0, False)


def publish_ddata(ddata, mid):
    while mid <= AMOUNT_OF_PUBLISHED_TRIES:
        inbound_payload = sparkplug_b.getDdataPayload()
        __add_ddata_metric_from_json(inbound_payload, ddata)

        byte_array = bytearray(inbound_payload.SerializeToString())

        print("Publishing DDATA")
        client.publish(topic, byte_array, qos=1)

        # Sit and wait for inbound or outbound events
        for _ in range(50):
            time.sleep(.1)
            client.loop()


def __add_ddata_metric_from_json(inbound_payload, ddata):
    for metric in ddata['metrics']:
        addMetric(inbound_payload, metric['name'], metric['alias'], data_type_matching[metric['dataType']],
                  metric['value'])


def __add_dbirth_metric_from_json(inbound_payload, dbirth):
    for metric in dbirth['metrics']:
        addMetric(inbound_payload, metric['name'], metric['alias'], choice(list(data_type_matching.items())),
                  _generate_double)


def _get_message_data(file_name):
    with open(file_name + '.json') as f:
        data = json.load(f)
    return data


if __name__ == '__main__':
    message_data = _get_message_data(DDATA_FILE_NAME)

    print("Start")
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_publish = on_publish

    client.connect(brocker_host, 1883, 60)

    time.sleep(.1)
    client.loop()

    # Publish the birth certificates
    publish_dbirth(message_data)

    # Publish DDATA messages
    publish_ddata(message_data)
