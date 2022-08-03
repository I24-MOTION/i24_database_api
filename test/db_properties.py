#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 24 11:02:50 2022

@author: yanbing_wang
"""
from src.i24_database_api import DBClient
from i24_configparse import parse_cfg
import os
import json

# parameters path
if __name__ == '__main__':
    with open("config/db_param.json") as f:
        parameters = json.load(f)
    
    # parameters["database_name"] = None
    # parameters["collection_name"] = ""
    client = DBClient(**parameters, database_name = "reconciled", collection_name="", latest_collection=True)
    print(client.collection_name)
    
    #%%
    # dbr.create_index(["ID", "first_timestamp", "last_timestamp", "starting_x", "ending_x"])
    # print("collection name: ", client.collection_name)
    # print("number of traj: ", client.count())
    
    # print("min start time: ", client.get_min("first_timestamp"))
    # print("max start time: ", client.get_max("first_timestamp"))
    
    # print("min end time: ", client.get_min("last_timestamp"))
    # print("max end time: ", client.get_max("last_timestamp"))
    
    # print("min start x: ", client.get_min("starting_x"))
    # print("max start x: ", client.get_max("starting_x"))
    
    # print("min ending x: ", client.get_min("ending_x"))
    # print("max ending x: ", client.get_max("ending_x"))
    
    # print(client.list_collection_names())
    
    
    #%%
    # client.transform()
    # col = client.get_latest_collection()
    
    #
    # col_list = ['morose_panda--RAW_GT1_cajoles','morose_panda--RAW_GT1_castigates',
    #             'morose_panda--RAW_GT1_disputes','morose_panda--RAW_GT1_boggles']
    # newdb = client.client["reconciled"]
    # for col in col_list:
    #     newcol = newdb[col]
    #     collection = client.db[col]
    #     for doc in collection.find({}):
    #         newcol.insert_one(doc)
    #     collection.drop()
    #     print("dropped ", col)
                
    
    
    
    
    
    
    
    
    
