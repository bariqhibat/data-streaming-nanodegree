
"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

import requests

from models.producer import Producer


logger = logging.getLogger(__name__)

REST_PROXY_URL_DOCKER = "http://rest-proxy:8082/"

class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://localhost:8082"

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        #
        #
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        #
        self.topic_name = f"com.udacity.dsnd.weather", # !DONE. TODO: Come up with a better topic name

        self.consumer_group = f"solution7-consumer-group-{random.randint(0,10000)}"

        super().__init__(
            self.topic_name,
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        #
        # TODO: Define this value schema in `schemas/weather_value.json
        # !DONE
        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        #
        #
        # TODO: Complete the function by posting a weather event to REST Proxy. Make sure to
        # specify the Avro schemas and verify that you are using the correct Content-Type header.
        #
        #
        """Consumes from REST Proxy"""
        consumer_name = "solution7-consumer"
        headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}
        data = {"name": consumer_name, "format": "avro"}
        resp = requests.post(
            f"{REST_PROXY_URL_DOCKER}/consumers/{self.consumer_group}",
            data=json.dumps(data),
            headers=headers,
        )

        try:
            resp.raise_for_status()
        except:
            print(
                f"Failed to create REST proxy consumer: {json.dumps(resp.json(), indent=2)}"
            )
            return
        
        print("REST Proxy consumer group created")

        resp_data = resp.json()
        #
        data = {"topics": [self.topic_name]}
        resp = requests.post(
            f"{resp_data['base_uri']}/subscription", data=json.dumps(data), headers=headers
        )
        try:
            resp.raise_for_status()
        except:
            print(
                f"Failed to subscribe REST proxy consumer: {json.dumps(resp.json(), indent=2)}"
            )
            return
        print("REST Proxy consumer subscription created")
        while True:
            #
            # TODO: Set the Accept header to the same data type as the consumer was created with
            #       See: https://docs.confluent.io/current/kafka-rest/api.html#get--consumers-(string-group_name)-instances-(string-instance)-records
            #
            headers = {"Accept": "application/vnd.kafka.avro.v2+json"}
            #
            # TODO: Begin fetching records
            #       See: https://docs.confluent.io/current/kafka-rest/api.html#get--consumers-(string-group_name)-instances-(string-instance)-records
            #
            resp = requests.get(f"{resp_data['base_uri']}/records", headers=headers)
            try:
                resp.raise_for_status()
            except:
                print(
                    f"Failed to fetch records with REST proxy consumer: {json.dumps(resp.json(), indent=2)}"
                )
                return
            print("Consumed records via REST Proxy:")
            print(f"{json.dumps(resp.json())}")

            logger.debug(
                "sent weather data to kafka, temp: %s, status: %s",
                self.temp,
                self.status.name,
            )
