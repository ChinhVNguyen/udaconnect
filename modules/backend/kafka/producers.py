import os
from kafka import KafkaProducer
kafka_server = os.environ["KAFKA_API"]
kafka_topic = os.environ["KAFKA_TOPIC"]

producer = KafkaProducer(bootstrap_servers=kafka_server)
msg1=dict({'first_name':'Ket','last_name':'Nguyen','company_name':'FPT'})
producer.send('person', bytes(str(msg1), 'utf-8'))
producer.flush()