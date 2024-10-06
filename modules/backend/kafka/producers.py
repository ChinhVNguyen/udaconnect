from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
msg1=dict({'first_name':'uda','last_name':'app','company_name':'udacity'})
producer.send('testing message', bytes(str(msg1), 'utf-8'))
producer.flush()