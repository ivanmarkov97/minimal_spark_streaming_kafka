from kafka import KafkaConsumer


if __name__ == '__main__':
    topic_name = 'my-topic'
    consumer = KafkaConsumer(topic_name)

    print(f'Started listening topic: {topic_name}')

    for message in consumer:
        print(f'Consumer from topic: {topic_name} | {message}')
