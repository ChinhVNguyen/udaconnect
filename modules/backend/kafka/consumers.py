from __future__ import annotations
from kafka import KafkaConsumer
from dataclasses import dataclass
from datetime import datetime
from person_pb2 import PersonMessage
from location_pb2 import LocationMessage
import person_pb2
import person_pb2_grpc
import location_pb2
import location_pb2_grpc
import json
import os
import ast
import grpc


def create_person(req):
    channel = grpc.insecure_channel('grpc-api:5003')
    stub = person_pb2_grpc.PersonServiceStub(channel)
    person = PersonMessage(first_name=req["first_name"] , last_name=req["last_name"], company_name=req["company_name"])
    stub.create_person(person)


def create_location(req):
    channel = grpc.insecure_channel('grpc-api:5003')
    stub = location_pb2_grpc.LocationServiceStub(channel)
    location = LocationMessage(person_id=req["person_id"], creation_time=req["creation_time"],latitude=req["latitude"],longitude=req["longitude"])
    stub.create_location(location)

kafka_api = os.getenv('KAFKA_API')
kafka_topics = os.getenv('KAFKA_TOPICS').split(',')

consumer = KafkaConsumer(*kafka_topics, bootstrap_servers=kafka_api, value_deserializer=lambda m: json.dumps(m.decode('utf-8')))

for message in consumer:
    resp=eval(json.loads((message.value)))
    if message.topic == 'create_person':
        create_person(resp)
    elif message.topic == 'create_location':
        create_location(resp)
    else:
        print(resp)