import os
import sys
import time
import logging
import json

import paho.mqtt.client as mqtt
from Sparkplug.client_libraries.python import sparkplug_b
from Sparkplug.client_libraries.python.sparkplug_b import *

DBIRTH_FILE_NAME = 'dbirth'
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


def publish_device_birth(dbirth):
    print("Publishing Device Birth")

    # Get the payload
    payload = sparkplug_b.getDeviceBirthPayload()

    # Add some device metrics
    __add_metric_from_json(payload, dbirth)

    # Publish the initial data with the Device BIRTH certificate
    total_byte_array = bytearray(payload.SerializeToString())
    client.publish(topic, total_byte_array, 0, False)


def publish_ddata(ddata):
    while True:
        inbound_payload = sparkplug_b.getDdataPayload()
        __add_metric_from_json(inbound_payload, ddata)

        byte_array = bytearray(inbound_payload.SerializeToString())

        print("Publishing DDATA")
        client.publish(topic, byte_array, qos=1)

        # Sit and wait for inbound or outbound events
        for _ in range(50):
            time.sleep(.1)
            client.loop()


def __add_metric_from_json(inbound_payload, message_data):
    for metric in message_data['metrics']:
        addMetric(inbound_payload, metric['name'], metric['alias'], data_type_matching[metric['dataType']],
                  metric['value'])


def _get_message_data(file_name):
    with open(file_name + '.json') as f:
        data = json.load(f)
    return data


if __name__ == '__main__':
    dbirth = _get_message_data(DBIRTH_FILE_NAME)
    ddata = _get_message_data(DDATA_FILE_NAME)

    print("Start")
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_publish = on_publish

    client.connect(brocker_host, 1883, 60)

    time.sleep(.1)
    client.loop()

    # Publish the birth certificates
    publish_device_birth(dbirth)

    # Publish DDATA messages
    publish_ddata(ddata)
