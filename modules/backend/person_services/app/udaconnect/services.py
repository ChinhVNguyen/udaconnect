import json
import logging
from datetime import datetime, timedelta
import os
from typing import Dict, List

from app import db
from app.udaconnect.models import Person
from app.udaconnect.schemas import PersonSchema
from geoalchemy2.functions import ST_AsText, ST_Point
from sqlalchemy.sql import text
from kafka import KafkaProducer

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("person-services")



class PersonService:
    @staticmethod
    def create(person: Dict) -> Person:
        kafka_api = os.getenv('KAFKA_API')
        kafka_topics = os.getenv('KAFKA_TOPICS').split(',')

        producer = KafkaProducer(
            bootstrap_servers=kafka_api,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        new_person = Person()
        new_person.first_name = person["first_name"]
        new_person.last_name = person["last_name"]
        new_person.company_name = person["company_name"]
        producer.send('persons', bytes(str(person), 'utf-8'))
        producer.flush()
        return new_person

    @staticmethod
    def retrieve(person_id: int) -> Person:
        person = db.session.query(Person).get(person_id)
        return person

    @staticmethod
    def retrieve_all() -> List[Person]:
        return db.session.query(Person).all()