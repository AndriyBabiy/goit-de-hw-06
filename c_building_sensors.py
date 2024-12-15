from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json
import time

# Creating Kafka consumer
consumer = KafkaConsumer (
  bootstrap_servers=kafka_config['bootstrap_servers'],
  security_protocol=kafka_config['security_protocol'],
  sasl_mechanism=kafka_config['sasl_mechanism'],
  sasl_plain_username=kafka_config['username'],
  sasl_plain_password=kafka_config['password'],
  value_deserializer=lambda v: json.loads(v.decode('utf-8')),
  key_deserializer=lambda v: json.loads(v.decode('utf-8')),
  auto_offset_reset='earliest', # read from start
  enable_auto_commit=True #Systematic confirmation of read messages
  # group_id='my_consumer_group'
)

# Creating a Kafka producer that will furhter produce notifications based on conditions
producer = KafkaProducer (
  bootstrap_servers=kafka_config['bootstrap_servers'],
  security_protocol=kafka_config['security_protocol'],
  sasl_mechanism=kafka_config['sasl_mechanism'],
  sasl_plain_username=kafka_config['username'],
  sasl_plain_password=kafka_config['password'],
  value_serializer=lambda v: json.dumps(v).encode('utf-8'),
  key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Topic name
my_name='andriy_b'
building_topic=f'{my_name}_building_sensors'
humidity_topic=f'{my_name}_humidity_alert'
temperature_topic=f'{my_name}_temperature_alert'

# Subscribe to topic
consumer.subscribe([building_topic])
print(f"'Subscribed to topic '{building_topic}'")

# Processing messages from topic
try:
  for message in consumer:
    print()
    print(f"recieve meassage: {message.value}, with key: {message.key}, partition: {message.partition} ")

    if (message.value['temperature'] > 40):
      data = {
        "timestamp": time.time(),
        "incident_timestamp": message.value['timestamp'],
        "temperature": message.value['temperature']
      }

      producer.send(temperature_topic, key=message.key, value=data)
      producer.flush() # Await for all messages to be sent

    if (message.value['humidity'] < 20 or message.value['humidity'] > 80):
      data = {
        'timestamp': time.time(),
        'incident_timestamp': message.value['timestamp'],
        'humidity': message.value['humidity']
      }

      producer.send(humidity_topic, key=message.key, value=data)
      producer.flush() # Awaiting all messages to be sent

except Exception as e:
  print(f'An error occurred: {e}')
finally:
  consumer.close()
  producer.close()