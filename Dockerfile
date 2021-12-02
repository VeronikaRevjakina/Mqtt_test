FROM python:3.7
ENV BROCKER_HOST=172.17.0.2
ENV TOPIC=home-assistant/switch/1/on
RUN  mkdir WORK_REPO
RUN  cd  WORK_REPO
WORKDIR  /WORK_REPO
COPY ./requirements.txt /WORK_REPO/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY . /WORK_REPO
ADD test_mqtt_publish.py .
CMD ["python", "-u", "test_mqtt_publish.py"]