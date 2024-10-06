FROM python:3.8

WORKDIR .

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

EXPOSE 5003

COPY . .
CMD ["python", "main.py"]