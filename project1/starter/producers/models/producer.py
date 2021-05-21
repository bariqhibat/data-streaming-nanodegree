"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

BROKER_URL_DOCKER = "PLAINTEXT://kafka0:9092,PLAINTEXT://kafka1:9093,PLAINTEXT://kafka2:9094	"
AVRO_SCHEMA_REGISTRY = "http://schema-registry:8081/"

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        # ! DONE
        #
        #
        self.broker_properties = {
            # TODO
            "linger.ms": "10000",
            "bootstrap.server": BROKER_URL_DOCKER,
            "batch.num.messages": "10000",
            "queue.buffering.max.messages": "1000000",
            "queue.buffering.max.kbytes": "1000",
            "compression.type": "lz4"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer 
        # ! DONE
        self.producer = AvroProducer(
            {
                'bootstrap.servers': BROKER_URL_DOCKER,
                'on_delivery': self.delivery_report,
                'schema.registry.url': AVRO_SCHEMA_REGISTRY
            }, 
            default_key_schema=self.key_schema, 
            default_value_schema=self.value_schema
        )

    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def topic_exists(self, client, topic_name):
        """Checks if the given topic exists"""
        topic_metadata = client.list_topics(timeout=5)
        return topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))

    def get_client(self):
        client = AdminClient({"bootstrap.servers": BROKER_URL_DOCKER})

        return client


    def create_topic(self, topic_name):
        """Creates the topic with the given topic name"""
        # ! DONE

        client = self.get_client()

        isExist = self.topic_exists(client, topic_name=topic_name)

        if isExist:
            return None

        futures = client.create_topics(
            [
                NewTopic(
                    topic=topic_name,
                    num_partitions=10,
                    replication_factor=1,
                    config={
                        "cleanup.policy": "delete",
                        "compression.type": "lz4",
                        "delete.retention.ms": "2000",
                        "file.delete.delay.ms": "2000",
                    },
                )
            ]
        )

        for _, future in futures.items():
            try:
                future.result()
                print("topic created")
            except Exception as e:
                print(f"failed to create topic {topic_name}: {e}")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        # ! DONE
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
