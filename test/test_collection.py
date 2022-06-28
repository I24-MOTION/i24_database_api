#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 21:45:29 2022

@author: yanbing_wang
"""
from src.i24_database_api.db_reader import DBReader
from src.i24_database_api.db_writer import DBWriter
from i24_configparse import parse_cfg
import matplotlib.pyplot as plt
import os

cwd = os.getcwd()
cfg = "config"
config_path = os.path.join(cwd,cfg)
os.environ["user_config_directory"] = config_path
os.environ["my_config_section"] = "DEFAULT"
db_param = parse_cfg("my_config_section", cfg_name = "database_param.config")

collection_name = "garbage_dump_2"

# dbw = DBWriter(host=db_param.default_host, port=db_param.default_port, 
#         username=db_param.default_username, password=db_param.default_password,
#         database_name=db_param.db_name, collection_name=collection_name,
#         server_id=1, process_name=1, process_id=1, session_config_id=1, schema_file=None)

dbr = DBReader(db_param, collection_name=collection_name)

dbr.create_index(["first_timestamp", "last_timestamp", "starting_x", "ending_x"])
print("collection name: ", collection_name)
print("number of traj: ", dbr.count())

# print("min ID: ", dbr.get_min("ID"))
# print("max ID: ", dbr.get_max("ID"))

print("min start time: ", dbr.get_min("first_timestamp"))
print("max start time: ", dbr.get_max("first_timestamp"))

print("min end time: ", dbr.get_min("last_timestamp"))
print("max end time: ", dbr.get_max("last_timestamp"))

print("min start x: ", dbr.get_min("starting_x"))
print("max start x: ", dbr.get_max("starting_x"))

print("min ending x: ", dbr.get_min("ending_x"))
print("max ending x: ", dbr.get_max("ending_x"))

import random
doc = dbr.collection.find()[random.randrange(dbr.count())]
# print(doc)
print(dbr.collection.distinct("direction"))
plt.figure()
plt.scatter(doc["timestamp"], doc["x_position"])
plt.title(doc["_id"])