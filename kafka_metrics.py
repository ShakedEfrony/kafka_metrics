from flask import Flask, request
import kafka
from datetime import datetime
import consts

# Initializing the flask application
app = Flask(__name__)

# Keep json keys order
app.json.sort_keys = False


# Setting up a route for the root URL
@app.route('/', methods=['GET'])
def get_topic_info():
    """Get topic's info.
    :return: a dict which contains the topic's info.
              The flask converts the dict to a json object
    """
    # Getting parameters from the client
    topic_name = request.args.get('topic_name', type=str)
    group_id = request.args.get('group_id', type=str)
    validate_parameters(group_id, topic_name)

    # Setting up consumer of kafka
    consumer = kafka.KafkaConsumer(
        group_id=group_id,
        bootstrap_servers=consts.BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=False  # We don't want to effect offsets
    )
    try:
        topic_info = __get_topic_info(consumer, topic_name, group_id)
        return topic_info
    finally:
        consumer.close()


def validate_parameters(group_id, topic_name):
    """Validate parameters from the client
    """
    if group_id is None or group_id == '':
        raise ValueError("group_id parameter is required")
    if topic_name is None or topic_name == '':
        raise ValueError("topic_name parameter is required")


def __get_topic_info(consumer, topic_name, group_id):
    """Get all the topic's data
    :return: topic's data
    """
    partition_ids = consumer.partitions_for_topic(topic_name)
    partitions_metrics = []  # define all the information about partitions - in a list of Dictionaries

    if topic_name not in consumer.topics():
      raise ValueError(f"The topic {topic_name} does not exist")

    for partition_id in partition_ids:
        partitions_metrics.append(get_partition_metrics(consumer, partition_id, topic_name))

    topic_info = {
        'topic_name': topic_name,
        'dt': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'group_id': group_id,
        "partitions": partitions_metrics
    }
    return topic_info


def get_partition_metrics(consumer, partition_id, topic_name):
    """Getting data about a topic's partition
    :return: a Dict with all the partition's metrics
    """
    partition = kafka.TopicPartition(topic_name, partition_id)
    consumer.assign([partition])

    committed_offset = consumer.committed(partition)  # Get last committed offset for this partition

    consumer.seek_to_end(partition)
    end_offset = consumer.position(partition)  # Get the current offset

    lag = end_offset - committed_offset if committed_offset is not None else end_offset

    return {
        'max_offsets': end_offset,
        'last_committed_offsets': committed_offset,
        'lag': lag,
        "partition_id": partition_id
    }


if __name__ == '__main__':
    app.run(debug=True)
