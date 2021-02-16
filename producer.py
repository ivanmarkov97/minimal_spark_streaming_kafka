import json
import time
import random
import string

from kafka import KafkaProducer


def on_send_success(record_metadata):
    print('Send success!')
    print('Topic', record_metadata.topic)
    print('Partition', record_metadata.partition)
    print('Offset', record_metadata.offset)


if __name__ == '__main__':
    topic_name = 'my-topic'
    message_tmp = {
        'time': None,
        'message': 'Hello from Producer',
        'symbols': [random.choice(string.ascii_letters) for _ in range(10)]
    }

    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    while True:
        time.sleep(5)
        message_tmp['time'] = time.localtime()
        print(f'Sending to topic {topic_name}')
        bytes_message = json.dumps(message_tmp).encode()
        producer.send(topic_name, value=bytes_message).add_callback(on_send_success)
