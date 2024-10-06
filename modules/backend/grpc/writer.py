import grpc
import person_pb2
import person_pb2_grpc
from person_pb2 import PersonMessage

from datetime import datetime

# open a gRPC channel
channel = grpc.insecure_channel('localhost:5003')

# create a stub (client)
stub = person_pb2_grpc.PersonServiceStub(channel)

# create a valid request message
person = PersonMessage(first_name="Ken" , last_name="Nguyen", company_name="FPT")
stub.create_person(person)
print(person)