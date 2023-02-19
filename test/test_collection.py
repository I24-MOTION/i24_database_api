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


collection_name = "tracking_v1_stitched"





cwd = os.getcwd()
cfg = "config"
config_path = os.path.join(cwd,cfg)
os.environ["USER_CONFIG_DIRECTORY"] = config_path
os.environ["test_config_section"] = "DEFAULT"
db_param = parse_cfg("test_config_section", cfg_name = "database_param.config")


dbr = DBReader(db_param, collection_name=collection_name)

# dbr.create_index(["first_timestamp", "last_timestamp", "starting_x", "ending_x"])
# print("collection name: ", collection_name)
# print("number of traj: ", dbr.count())

# # print("min ID: ", dbr.get_min("ID"))
# # print("max ID: ", dbr.get_max("ID"))

# print("min start time: ", dbr.get_min("first_timestamp"))
# print("max start time: ", dbr.get_max("first_timestamp"))

# print("min end time: ", dbr.get_min("last_timestamp"))
# print("max end time: ", dbr.get_max("last_timestamp"))

# print("min start x: ", dbr.get_min("starting_x"))
# print("max start x: ", dbr.get_max("starting_x"))

# print("min ending x: ", dbr.get_min("ending_x"))
# print("max ending x: ", dbr.get_max("ending_x"))

# import random
# doc = dbr.collection.find()[random.randrange(dbr.count())]
# print(doc["y_position"])
# print(dbr.collection.distinct("direction"))
# plt.figure()
# plt.scatter(doc["timestamp"], doc["x_position"])
# plt.title(doc["_id"])


# Reset collection
# reconciled_schema_path = "config/reconciled_schema.json"
# dbw = DBWriter(db_param, database_name = "trajectories", collection_name = "groundtruth_scene_1", schema_file=reconciled_schema_path)


# print("groundtruth_scene_1" in dbw.db.list_collection_names())
# print(dbw.db.list_collection_names())
# print(dbw.collection.count_documents({}))


#%%  make a copy
# pipeline = [ {"$match": {}}, 
#              {"$out": "groundtruth_scene_1_copy"},
# ]
# dbw.collection.aggregate(pipeline)

#%% batch update from list of length to double
# from pymongo import UpdateOne
# dbw = DBWriter(db_param, database_name = "trajectories", collection_name = "groundtruth_scene_1", schema_file=None)
# col = dbw.db["groundtruth_scene_1"]


# batch=[]
# for d in col.find({}):
#     batch.append(
#         UpdateOne(
#             {'_id':d["_id"]}, 
#             {
#                 "$set":
#                     {
#                         "length":d["length"][0] ,
#                         "width":d["width"][0] ,
#                         "height":d["height"][0] 
#                     }, 
            
#             }, upsert=False)
#         )

# col.bulk_write(batch, ordered=False)

                    

