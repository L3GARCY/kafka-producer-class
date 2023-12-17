from confluent_kafka import Producer, Consumer, KafkaError

# Kafka broker configuration
bootstrap_servers = 'your_kafka_bootstrap_servers'

# Example producer
def produce_messages(topic, messages):
    producer_conf = {
        'bootstrap.servers': bootstrap_servers,
        # Add more configuration parameters as needed
    }

    producer = Producer(producer_conf)

    for message in messages:
        # Produce the message to the specified topic
        producer.produce(topic, key=None, value=message)

    # Wait for any outstanding messages to be delivered and delivery reports
    producer.flush()

# Example consumer
def consume_messages(topic):
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'your_consumer_group_id',
        'auto.offset.reset': 'earliest',  # Start reading from the beginning of the topic if no offset is stored
        # Add more configuration parameters as needed
    }

    consumer = Consumer(consumer_conf)
    
    # Subscribe to the specified topic
    consumer.subscribe([topic])

    try:
        while True:
            # Poll for messages
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event - not an error
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break

            # Process the received message
            print(f"Received message: {msg.value().decode('utf-8')}")

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

# Example usage
topic_name = 'your_topic_name'

# Example producer usage
messages_to_produce = ['Message 1', 'Message 2', 'Message 3']
produce_messages(topic_name, messages_to_produce)

# Example consumer usage
consume_messages(topic_name)
