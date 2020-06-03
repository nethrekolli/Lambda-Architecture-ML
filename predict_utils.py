import sys, os, re
import pymongo
import datetime, iso8601

def get_flight_distance(client, origin, dest):
  """Get the distance between a pair of airport codes"""
  query = {
    "origin": origin,
    "dest": dest,
  }
  record = client.agile_data_science.origin_dest_distances.find_one(query)
  return record["distance"]

def get_current_timestamp():
  iso_now = datetime.datetime.now().isoformat()
  return iso_now
