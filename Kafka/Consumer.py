#!/usr/bin/env python
from __future__ import print_function
import threading, logging, time

from kafka import KafkaConsumer, KafkaProducer
import json

import Application


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='127.0.0.1:9091',
                                 auto_offset_reset='earliest',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe(['recommendation_questionnaire'])

        Application.run()
        for message in consumer:
            print(message)
