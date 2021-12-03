# Mqtt brocker test with publisher

In order to test you need to set up separate docker container as mqtt brocker:


**docker run --name mq -it -p 1883:1883 -p 9001:9001  eclipse-mosquitto**

Make sure you fix mosquitto.conf to set allow_anonymoys true (https://stackoverflow.com/questions/66143452/unable-to-connect-from-outside-the-mosquitto-docker)

Then create image and load docker container for publisher providing mqtt brocker host and topic:


**docker build -t my_mqtt**

**docker run -e BROCKER_HOST=172.17.0.2 -e TOPIC=home-assistant/switch/1/on my_mqtt**

Then subscribe to the topic you provide in parameters:

**docker run --rm -it   --link mq   ruimarinho/mosquitto mosquitto_sub -h mq -t "#"**

and you'll see ddata.json messages in sparkplug format through subscriber.
