from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

# Creating a Kafka producer
producer = KafkaProducer (
  bootstrap_servers=kafka_config['bootstrap_servers'][0],
  security_protocol=kafka_config['security_protocol'],
  sasl_mechanism=kafka_config['sasl_mechanism'],
  sasl_plain_username=kafka_config['username'],
  sasl_plain_password=kafka_config['password'],
  value_serializer=lambda v: json.dumps(v).encode('utf-8'),
  key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Topic info
my_name='andriy_b'
building_topic=f'{my_name}_building_sensors'
humidity_topic=f'{my_name}_humidity_alert'
temperature_topic=f'{my_name}_temperature_alert'

# Running the producer
try:
  for i in range(50):
    time.sleep(2)
    data = {
      "timestamp": time.time(),
      "temperature": random.randint(25, 45),
      "humidity": random.randint(15, 85)
    }

    producer.send(building_topic, key=str(uuid.uuid4()), value=data)
    producer.flush() # Awaits all messages to be sent
    print(f"Data {data} collected and sent to topic '{building_topic}' successfully.")
except Exception as e:
  print(f"An error occurred: {e}")

producer.close()
