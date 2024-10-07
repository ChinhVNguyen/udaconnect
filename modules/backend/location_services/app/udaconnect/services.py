import json
import logging
from datetime import datetime, timedelta
import os
from typing import Dict, List
import time
from app import db
from app.udaconnect.models import Connection, Location, Person
from app.udaconnect.schemas import ConnectionSchema, LocationSchema, PersonSchema
from geoalchemy2.functions import ST_AsText, ST_Point
from sqlalchemy.sql import text
from kafka import KafkaProducer


logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("location-services")


class LocationService:
    @staticmethod
    def retrieve(location_id) -> Location:
        location, coord_text = (
            db.session.query(Location, Location.coordinate.ST_AsText())
            .filter(Location.id == location_id)
            .one()
        )

        # Rely on database to return text form of point to reduce overhead of conversion in app code
        location.wkt_shape = coord_text
        return location

    @staticmethod
    def create(location: Dict) -> Location:
        validation_results: Dict = LocationSchema().validate(location)
        if validation_results:
            logger.warning(f"Unexpected data format in payload: {validation_results}")
            raise Exception(f"Invalid payload: {validation_results}")

        kafka_api = os.getenv('KAFKA_API')
        kafka_topics = os.getenv('KAFKA_TOPICS').split(',')

        producer = KafkaProducer(
            bootstrap_servers=kafka_api,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
       
        new_location = Location()
        new_location.person_id = location["person_id"]
        new_location.creation_time = location.get("creation_time", datetime.utcnow())  # Use current time if not provided
        new_location.coordinate = ST_Point(location["latitude"], location["longitude"])

        location_dict = LocationSchema().dump(new_location)

        producer.send('locations', value=location_dict) 

        producer.flush()
        return new_location