from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Create Kafka client
admin_client = KafkaAdminClient(
  bootstrap_servers=kafka_config['bootstrap_servers'],
  security_protocol=kafka_config['security_protocol'],
  sasl_mechanism=kafka_config['sasl_mechanism'],
  sasl_plain_username=kafka_config['username'],
  sasl_plain_password=kafka_config['password']
)

# Outlining new topic
my_name = "andriy_b"
building_topic = f'{my_name}_building_sensors'
humidity_topic = f'{my_name}_humidity_alert'
temperature_topic = f'{my_name}_temperature_alert'
num_partitions = 2
replication_factor = 1

building_topic = NewTopic(name=building_topic, num_partitions=num_partitions, replication_factor=replication_factor)
humidity_topic = NewTopic(name=humidity_topic, num_partitions=num_partitions, replication_factor=replication_factor)
temperature_topic = NewTopic(name=temperature_topic, num_partitions=num_partitions, replication_factor=replication_factor)

topics = [building_topic, humidity_topic, temperature_topic]

# Creating the topic(s)
try:
  admin_client.create_topics(new_topics=topics, validate_only=False)
  for topic in topics:
    print(f"Topic '{topic}' created successfully")
except Exception as e:
  print(f"An error occurred: {e}")

# Getting the list of existing topics
for topic in admin_client.list_topics():
  if 'YT_' in topic:
    print(topic)

admin_client.close()