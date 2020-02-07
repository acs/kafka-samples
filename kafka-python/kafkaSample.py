import logging
# You must install kafka-python not kafka
import sys
import time

from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.cluster import ClusterMetadata
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

KAFKA_URL = "127.0.0.1"
KAFKA_TOPIC_FAST = "topic_fast"
KAFKA_TOPIC_SLOW = "topic_slow"
RETENTION_KEY = "retention.ms"
RETENTION_POLICY_KEY = "cleanup.policy"

# TODO: Explore https://aiokafka.readthedocs.io/en/stable/


def create_topic(admin_client, new_topic):
    # Let's create a topic with retention to check how to collect the data

    # Show how to configure topics features like retention
    topic_list = [NewTopic(new_topic, num_partitions=1, replication_factor=1,
                           topic_configs={'retention.ms': '168'})]

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except TopicAlreadyExistsError:
        logging.error("topic %s already exists" % new_topic)


def delete_topic(admin_client, topic):

    try:
        admin_client.delete_topics([topic])
    except UnknownTopicOrPartitionError:
        logging.warning("Can not delete " + topic + ": it does not exist")


def find_topic_partitions(consumer, topic):
    partitions = consumer.partitions_for_topic(topic)
    consumer.close()
    return partitions


def find_topic_config(admin_client, topic=None):
    config_data = {}
    configs = admin_client.describe_configs(config_resources=[ConfigResource(ConfigResourceType.TOPIC, KAFKA_TOPIC_FAST)])
    config_list = configs.resources[0][4]
    for config_tuple in config_list:
        config_data[config_tuple[0]] = config_tuple[1]
    return config_data


def produce_data(topic):
    pass


def consume_data(topic):
    pass


if __name__ == '__main__':
    logging.basicConfig()

    admin_client = KafkaAdminClient(bootstrap_servers=[KAFKA_URL])
    consumer = KafkaConsumer(bootstrap_servers=[KAFKA_URL])

    delete_topic(admin_client, KAFKA_TOPIC_SLOW)
    delete_topic(admin_client, KAFKA_TOPIC_FAST)

    time.sleep(0.1)  # give time to kafka to remove the topics

    create_topic(admin_client, KAFKA_TOPIC_SLOW)
    create_topic(admin_client, KAFKA_TOPIC_FAST)

    config_slow = find_topic_config(admin_client, KAFKA_TOPIC_SLOW)
    config_fast = find_topic_config(admin_client, KAFKA_TOPIC_FAST)

    print("%s for %s: %s" % (RETENTION_KEY, KAFKA_TOPIC_SLOW, config_slow[RETENTION_KEY]))
    print("%s for %s: %s" % (RETENTION_KEY, KAFKA_TOPIC_FAST, config_fast[RETENTION_KEY]))

    print("%s for %s: %s" % (RETENTION_POLICY_KEY, KAFKA_TOPIC_SLOW, config_slow[RETENTION_POLICY_KEY]))
    print("%s for %s: %s" % (RETENTION_POLICY_KEY, KAFKA_TOPIC_FAST, config_fast[RETENTION_POLICY_KEY]))

    topics = consumer.topics()
    for topic in topics:
        if topic in [KAFKA_TOPIC_FAST, KAFKA_TOPIC_SLOW]:
            print("Found created topic: " + topic)

    consumer.close()
    admin_client.close()
