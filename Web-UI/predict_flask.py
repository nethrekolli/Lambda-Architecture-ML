import sys, os, re
from flask import Flask, render_template, request
from pymongo import MongoClient
from cassandra.cluster import Cluster
from bson import json_util

# Configuration details
import config

# Helpers for search and prediction APIs
import predict_utils

# Set up Flask, Mongo and Elasticsearch
app = Flask(__name__)

client = MongoClient()

cluster = Cluster()
session = cluster.connect('lambda')

from pyelasticsearch import ElasticSearch
elastic = ElasticSearch(config.ELASTIC_URL)

import json

# Date/time stuff
import iso8601
import datetime

# Setup Kafka
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,9))
PREDICTION_TOPIC = 'flight_delay_classification_request'

import uuid

# Make our API a post, so a search engine wouldn't hit it
@app.route("/flights/delays/predict/classify_realtime", methods=['POST'])
def classify_flight_delays_realtime():
  """POST API for classifying flight delays"""
  
  # Define the form fields to process
  api_field_type_map = \
    {
      "dep_delay": int,
      "carrier": str,
      "FlightDate": str,
      "dest": str,
      "FlightNum": str,
      "origin": str,
      "route": str,
      "arr_time": int,
      "dep_time": int
    }

  # Fetch the values for each field from the form object
  api_form_values = {}
  for api_field_name, api_field_type in api_field_type_map.items():
    api_form_values[api_field_name] = request.form.get(api_field_name, type=api_field_type)
  
  # Set the direct values, which excludes Date
  prediction_features = {}
  for key, value in api_form_values.items():
    prediction_features[key] = value
  
  # Set the derived values
  prediction_features['distance'] = predict_utils.get_flight_distance(
    client, api_form_values['origin'],
    api_form_values['dest']
  )
  
  # Turn the date into DayOfMonth, DayOfWeek
  date_features_dict = predict_utils.get_regression_date_args(
    api_form_values['FlightDate']
  )
  for api_field_name, api_field_value in date_features_dict.items():
    prediction_features[api_field_name] = api_field_value
  
  # Create a unique ID for this message
  unique_id = str(uuid.uuid4())
  prediction_features['UUID'] = unique_id

  # Add a timestamp
  prediction_features['Timestamp'] = predict_utils.get_current_timestamp()
  
  print(json.dumps(prediction_features, sort_keys=True, indent=4))
  message_bytes = json.dumps(prediction_features).encode()
  producer.send(PREDICTION_TOPIC, message_bytes)

  response = {"status": "OK", "id": unique_id}
  return json_util.dumps(response)

@app.route("/flights/delays/predict_kafka")
def flight_delays_page_kafka():
  """Serves flight delay prediction page with polling form"""
  
  form_config = [
    {'field': 'dep_delay', 'label': 'Departure Delay'},
    {'field': 'carrier', 'label': 'Carrier'},
    {'field': 'FlightDate', 'label': 'Date'},
    {'field': 'origin', 'label': 'Origin'},
    {'field': 'dest', 'label': 'Destination'},
    {'field': 'route', 'label': 'Route'},
    {'field': 'arr_time', 'label': 'Arrival Time'},
    {'field': 'dep_time', 'label': 'Departure Time'},
  ]
  
  return render_template('flight_delays_predict_kafka.html', form_config=form_config)

@app.route("/flights/delays/predict/classify_realtime/response/<unique_id>")
def classify_flight_delays_realtime_response(unique_id):
  """Serves predictions to polling requestors"""

  query = session.prepare("SELECT prediction from flight_delay_classification_response where UUID=?")
  prediction = session.execute(query, [unique_id])

  response = {}
  response["status"]= "WAIT"
  response["id"]= unique_id
  if prediction:
    response["status"] = "OK"
    response["prediction"] = prediction
  
  return json_util.dumps(response)

def shutdown_server():
  func = request.environ.get('werkzeug.server.shutdown')
  if func is None:
    raise RuntimeError('Not running with the Werkzeug Server')
  func()

@app.route('/shutdown')
def shutdown():
  shutdown_server()
  return 'Server shutting down...'

if __name__ == "__main__":
    app.run(
    debug=True,
    host='0.0.0.0'
  )
