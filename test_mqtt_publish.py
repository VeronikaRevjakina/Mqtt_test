import sys
import paho.mqtt.client as mqtt
import time
import random
import os

from Sparkplug.client_libraries.python import sparkplug_b
from Sparkplug.client_libraries.python.sparkplug_b import *

sys.path.insert(0, "../client_libraries/python/")

brocker_host = os.environ['BROCKER_HOST']
topic = os.environ['TOPIC']

# The callback for when the client receives a CONNACK response from the server.
def on_connect(mqttc, obj, flags, rc):
    print("Connected with result code " + str(rc))


def on_publish(mqttc, obj, mid):
    print("mid: " + str(mid))


def on_message(mqttc, obj, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))


def publishDeviceBirth():
    print("Publishing Device Birth")

    # Get the payload
    payload = sparkplug_b.getDeviceBirthPayload()

    # Add some device metrics
    addMetric(payload, "STA04_Stacking/Stacking/Cylinders/Track 4 -Lifting cylinder/Start to work positionoutput", 154,
              MetricDataType.Boolean, False)
    addMetric(payload,
              "STA02_Lens Placing/SuctionGrippers/Cylinders/Track 2 - Moving to the right/ERROR - Work positionbusy",
              155, MetricDataType.Boolean, False)

    # Publish the initial data with the Device BIRTH certificate
    totalByteArray = bytearray(payload.SerializeToString())
    client.publish(topic, totalByteArray, 0, False)


print("Start")
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_publish = on_publish

client.connect(brocker_host, 1883, 60)

time.sleep(.1)
client.loop()

# Publish the birth certificates
publishDeviceBirth()

while True:
    inboundPayload = sparkplug_b.getDdataPayload()
    addMetric(inboundPayload, "", 154, MetricDataType.Boolean, random.choice([True, False]))
    addMetric(inboundPayload, "", 155, MetricDataType.Boolean, random.choice([True, False]))
    byteArray = bytearray(inboundPayload.SerializeToString())

    print("publishing")
    infot = client.publish(topic, byteArray, qos=1)
    # infot.wait_for_publish()

    # Sit and wait for inbound or outbound events
    for _ in range(50):
        time.sleep(.1)
        client.loop()
